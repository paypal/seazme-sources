(comment
  "manual performance tests"
  (defn get-data-entries-seq-count[session-id pref] (->> (hb/scan* "datahub:intake-data" :starts-with (str pref "\\" session-id) :lazy? true) count))
  (def sessions (->> (hb/scan* "datahub:intake-sessions" :from nil :lazy? true) (filter (comp (partial = "request") :action :meta :self second))))
  (def sessions (->> (hb/scan* "datahub:intake-sessions" :from nil :lazy? true) (filter (comp (partial = "submit") :action :meta :self second))))
  (def sessions (find-sessions nil))
  (def session-id (->> sessions (map (comp :id :meta :self)) last))
  (time (->> dist-bytes (pmap (partial get-data-entries-seq-count session-id)) (reduce +)))
  ;;"Elapsed time: 10291.724969 msecs" 66342
  (time (->> dist-bytes (pmap (partial get-data-entries-seq session-id)) (map count) (reduce +)))
  ;;"Elapsed time: 13049.982806 msecs" 66342
  (time (->> dist-bytes (pmap (partial get-data-entries-seq session-id)) (mapcat identity) count))
  ;;"Elapsed time: 13124.804484 msecs" 66342
)
