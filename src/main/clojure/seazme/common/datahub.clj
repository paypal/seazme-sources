(ns seazme.common.datahub
  (:require [seazme.common.hbase :as hb]
            [seazme.sources.twiki :as t]
            [seazme.sources.confluence :as c]
            [seazme.sources.jira :as j]
            [clj-uuid :as uuid]
            [clj-time.format :as tf] [clj-time.core :as tr] [clj-time.coerce :as te])
  (:use seazme.common.common))

(defn get-data-entries-seq[session-id pref] (hb/scan* "datahub:intake-data" :starts-with (str pref "\\" session-id) :lazy? true))
(def dist-bytes (map (partial format "%02x") (range 256)))

;;very important: part of structure, cannot change without regenerating indexes in ES
(def ts-prn-format (tf/formatter "YYYY/MM/dd HH:mm"))
(defn ts-prn[dt] (tf/unparse ts-prn-format (te/from-long dt)))
(def ts-key (tf/formatter "yyyyMMddHHmmss"))
(defn ts-fromts[dt] (tf/unparse ts-key (te/from-long dt)))
(defn app2index2[prefix app ts] (clojure.string/lower-case (format "%s-%s-%s" prefix (apply format "%s-%s-%s" (map app [:kind :bu :instance])) (ts-fromts ts))))

(defn default-parse-topic[kind bu instance base-url content] nil)

(def filter-action (comp (partial = "submit") :action :meta :self second))
(defn find-sessions-kv[last-update]
  (doall
   (if (nil? last-update)
     (->> (hb/scan* "datahub:intake-sessions" :lazy? true)
          (filter filter-action))
     (->> (hb/scan* "datahub:intake-sessions" :from last-update :lazy? true)
          (filter filter-action)
          rest))))
(defn find-sessions[last-update] (->> last-update find-sessions-kv (map second)))

(defn apps2map[apps] (->> apps (map (comp #(vector % nil) #(select-keys % [:bu :kind :instance]) :self second)) (into {})))


(defn sessions-last-scans[sessions]
  ;;naive version (->> ... (group-by first) (fmap #(->> % (map second) (sort-by :created) first)))
  (->> sessions
       (filter #(-> % :self :command (= "scan")))
       (map #(vector (select-keys (:app %) [:instance :bu :kind]) (select-keys (-> % :self :meta) [:tsx :created])));;TODO make those selects public functions
       (reduce
        (fn [m [k v]]
          (assoc m k v))
        {})))

(defn scan-session-pred[maxs session]
  (let [tsx (-> session :self :meta :tsx)
        app2 (-> session :app (select-keys [:instance :bu :kind]))]
    (neg? (compare tsx (-> app2 maxs :tsx))))) ;;tsx in session is newer than from update-log ;;TODO critical for UT

(defn remove-upto-last[maxs sessions]
  (remove (partial scan-session-pred maxs) sessions))

(defn remove-upto-last-scans[sessions]
  ;;double lazy scan is necessary here
  ;;there is also a slight chance, when scan happens in the middle
  ;;and unnecessary sessions will leak in
  (remove (partial scan-session-pred (sessions-last-scans sessions)) sessions))

(defn process-session![scan-action update-action [prev-tsx prev-created app2scan app2update] session]
  (let [self (-> session :self)
        app (-> session :app)
        u (select-keys app [:instance :bu :kind])
        command (-> self :command)
        tsx (-> self :meta :tsx)
        ts (-> self :meta :created)
        created (-> self :meta :created)
        id (-> self :meta :id)
        claimed-count (-> self :count)
        range2 (-> self :range)
        common-msg (str command " for " u " " id " " (->> range2 vals sort (map ts-prn) pr-str) " run on:" (ts-prn ts) "/" tsx)]
    (println "processing:" common-msg ", claimed count:" claimed-count)
    ;;tsx in session has to be newer than from update-log
    (if (pos? (compare tsx (app2update u)))
      (let [[new-app2scan count] (if (= "scan" command)
                                   [(assoc app2scan u ts) (scan-action ts (app2scan u) app session)]
                                   [app2scan (update-action ts (app2scan u) app session)])]
        (println " processed:" common-msg ", claimed count:" claimed-count ", actual count:" count)
        [tsx created new-app2scan (assoc app2update u tsx) count])
      (do
        (println "!processed:" common-msg ", actual count:" count)
        ;;TODO maybe an assert
        [tsx created app2scan app2update nil]))))

;; TODO it is still a singleton but we are way closer to fully distributed version
;; TODO check data sources index
;; TODO in case of backend error (e.g. ES) throw and exit, so it always can be restarted
(defn- reduce-update-log![process-session stf prefix action app2scan app2update sessions]
  ;;scan last entry in "datahub:snapshot-update-log" from "prefix-tsx" and or with apps
  ;; assumption that scan/update for app can only happen after app is created - hence we can take last tsx from scan
  ;;   - it could in nul meta tsx since it is not possible to create sessions in past ....
  ;;s1 u1 u1 u1    s2 u1 u2 u1 (s-scan, u-update)
  ;;            a2
  ;;n  n  n  n  ! !
  ;;l        l       l
  (let [reduce-id (uuid/v1)
        reduce-session-fn (fn[[prev-tsx prev-created prev-app2scan prev-app2update] session]
                            (let [jts1 (jts-now)
                                  [new-tsx new-created new-app2scan new-app2update cnt] (process-session [prev-tsx prev-created prev-app2scan prev-app2update] session)
                                  jts2 (jts-now)
                                  prefix2 (str prefix "-");;TODO validate prefix, it cannot contain "-"
                                  not-tsx (format "%011x" (bit-not new-created))
                                  key (str prefix2 not-tsx)
                                  self {:app2scan new-app2scan
                                        :app2update new-app2update
                                        :meta {:action action
                                               :reduce-id reduce-id
                                               :prefix prefix
                                               :key key
                                               :tsx new-tsx
                                               :created jts1
                                               :completed jts2
                                               :count cnt
                                               :session (session :self)
                                               }}]
                              (stf key self)
                              [new-tsx new-created new-app2scan new-app2update]))]
    (reduce reduce-session-fn [nil 0 app2scan app2update] sessions)))


(defn- process-update-log![prefix action process-session]
  ;; it is possible to filter sessions for regular update case however that would incur full scan on sessions table
  ;; which is less desireble than potential savings in certain corner cases
  (let [prefix2 (str prefix "-");;TODO validate prefix, it cannot contain "-", make it common
        stf (fn st[key self] (hb/store* "datahub:snapshot-update-log" key :self self))]
    (if (= "scan" action)
      (let [recent-sessions (remove-upto-last-scans (find-sessions nil))]
        (reduce-update-log! process-session stf prefix action {} {} recent-sessions))
      (if-let [updates-log-kv (seq (hb/scan* "datahub:snapshot-update-log" :starts-with prefix2 :lazy? true))]
        (let [tmp (-> updates-log-kv first second :self)
              tsx (-> tmp :meta :tsx)
              sessions (find-sessions tsx)]
          (reduce-update-log! process-session stf prefix action (-> tmp :app2scan) (-> tmp :app2update) sessions))
        (println "Please run scan first!")))))

(defn run-update-log![prefix action scan-action update-action]
  (process-update-log! prefix action (partial process-session! (partial scan-action prefix) (partial update-action prefix))))
