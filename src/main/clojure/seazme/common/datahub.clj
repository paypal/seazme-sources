(ns seazme.common.datahub
  (:require [seazme.common.hbase :as hb]
            [seazme.sources.twiki :as t]
            [seazme.sources.confluence :as c]
            [seazme.sources.jira :as j]))

(defn get-data-entries-seq[session-id pref] (hb/scan* "datahub:intake-data" :starts-with (str pref "\\" session-id) :lazy? true))
(def dist-bytes (map (partial format "%02x") (range 256)))
(defn app2index[prefix app] (clojure.string/lower-case (apply format "%s-%s-%s-%s" prefix (map app [:kind :bu :instance]))))

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
