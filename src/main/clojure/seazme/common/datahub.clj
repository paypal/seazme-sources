(ns seazme.common.datahub
  (:require [seazme.common.hbase :as hb]
            [seazme.sources.twiki :as t]
            [seazme.sources.confluence :as c]
            [seazme.sources.jira :as j]
            [clj-time.format :as tf] [clj-time.core :as tr] [clj-time.coerce :as te])
  (:use seazme.common.common))

(defn get-data-entries-seq[session-id pref] (hb/scan* "datahub:intake-data" :starts-with (str pref "\\" session-id) :lazy? true))
(def dist-bytes (map (partial format "%02x") (range 256)))

;;very important: part of structure, cannot change without regenerating indexes in ES
(def ts-key (tf/formatter "yyyyMMddHHmmss"))
(defn ts-fromts[dt] (tf/unparse ts-key (te/from-long dt)))
(defn app2index2[prefix app ts] (clojure.string/lower-case (format "%s-%s-%s" prefix (apply format "%s-%s-%s" (map app [:kind :bu :instance])) (ts-fromts ts))))

(defn default-parse-topic[kind bu instance base-url content] nil)

(def filter-action (comp (partial = "submit") :action :meta :self second))
(defn find-sessions[last-update]
  (doall
   (if (nil? last-update)
     (->> (hb/scan* "datahub:intake-sessions" :lazy? true)
          (filter filter-action))
     (->> (hb/scan* "datahub:intake-sessions" :from last-update :lazy? true)
          (filter filter-action)
          rest))))

(defn apps2map[apps] (->> apps (map (comp #(vector % nil) #(select-keys % [:bu :kind :instance]) :self second)) (into {})))


(defn process-update-log![process-session prefix]
  ;; in case of backend error (e.g. ES) throw and exit
  ;;
  ;;scan last entry in "datahub:snapshot-update-log" from "prefix-tsx" and or with apps
  ;; assumption that scan/update for app can only happen after app is created - hence we can take last tsx from scan
  ;;   - it could in sul meta tsx since it is not possible to create sessions in past ....
  ;;s1 u1 u1 u1    s2 u1 u2 u1 (s-scan, u-update)
  ;;            a2
  ;;n  n  n  n  ! !
  ;;l        l       l
  ;;TODO check data sources index
  ;;TODO it is still a singleton but we are way closer to fully distributed version
  (letfn [(x-fn [a2i a2u sessions-kv]
            (let [reduce-session-fn (fn[[old-tsx old-created old-a2i old-a2u] remaining-sessions-kv]
                                      (let [[new-tsx new-created new-a2i new-a2u] (process-session [old-tsx old-created old-a2i old-a2u] remaining-sessions-kv)
                                            jts (jts-now)
                                            not-tsx (format "%011x" (bit-not new-created))
                                            key (str prefix "-" not-tsx)] ;;TODO validate prefix, cannot contain "-"
                                        (hb/store* "datahub:snapshot-update-log" key :self
                                                   {:a2u new-a2u
                                                    :a2i new-a2i
                                                    :meta {:key key
                                                           :tsx new-tsx
                                                           :created jts}})
                                        [new-tsx new-created new-a2i new-a2u]))]
              (reduce reduce-session-fn [nil 0 a2i a2u] sessions-kv)))]
    (if-let [updates-log (seq (hb/scan* "datahub:snapshot-update-log" :starts-with prefix :lazy? true))]
      (let [last-update-log (-> updates-log first second :self)
            a2u (-> last-update-log :a2u)
            a2i (-> last-update-log :a2i)
            tsx (-> last-update-log :meta :tsx)
            sessions-kv (find-sessions tsx)]
        (x-fn a2i a2u sessions-kv))
      (let [apps (hb/scan* "datahub:apps")
            a2 (apps2map apps)
            sessions-kv (find-sessions nil)]
        (x-fn a2 a2 sessions-kv)))))

(comment
  "manual performance tests"
  (defn get-data-entries-seq-count[session-id pref] (->> (hb/scan* "datahub:intake-data" :starts-with (str pref "\\" session-id) :lazy? true) count))
  (def sessions (->> (hb/scan* "datahub:intake-sessions" :from nil :lazy? true) (filter (comp (partial = "request") :action :meta :self second))))
  (def sessions (->> (hb/scan* "datahub:intake-sessions" :from nil :lazy? true) (filter (comp (partial = "submit") :action :meta :self second))))
  (def sessions (find-sessions nil))
  (def session-id (->> sessions (map (comp :id :meta :self second)) last))
  (time (->> dist-bytes (pmap (partial get-data-entries-seq-count session-id)) (reduce +)))
  ;;"Elapsed time: 10291.724969 msecs" 66342
  (time (->> dist-bytes (pmap (partial get-data-entries-seq session-id)) (map count) (reduce +)))
  ;;"Elapsed time: 13049.982806 msecs" 66342
  (time (->> dist-bytes (pmap (partial get-data-entries-seq session-id)) (mapcat identity) count))
  ;;"Elapsed time: 13124.804484 msecs" 66342
)
