(comment
  "delete bad indices"
  (->> (esi/get-settings conn) keys (map name) (map (partial esi/delete conn)) doall frequencies)
  )

(comment
  "delete bad prefix"
  (def prefix "badprefix")
  (def to-del (->> (hb/scan* "datahub:snapshot-update-log" :starts-with prefix :lazy? true) (map (comp name first))))
  (->> to-del (map (partial delete (hb/get-conn) "datahub:snapshot-update-log")) dorun)
  )


(comment
  "run mapping"
  (use 'seazme.sources.snapshot :reload)
  (def payload (-> "<jirakey-1>" pja-i :body (json/read-str :key-fn keyword)))
  (pprint payload)
  (pprint (apply-field-mapping field-mapping payload))
  )

(common
 (require '[seazme.sources.jira-api :as jira-api])
 (def pja-search-api (jira-api/mk-pja-search-api (-> config/config :jira-pp-prod :url) (-> config/config :jira-pp-prod :credentials) true))
 (jira-api/pja-search-all-full pja-search-api "2019/04/23 18:34" "2019/04/23 18:35" #(-> % :key println))
 )

(comment
  "reading cache files"

  (def ese #(vector (-> % :key) (-> % :fields :updated) (-> % :fields :status :name)))

  ;; single
  (def files (->> "<your-dir>" clojure.java.io/file file-seq (filter #(.isFile %)) sort)) ;;TODO sort by file name
  (->> files first (scan-file ese) (sort-by second))

  ;;or diff
  (def f1 (->> "<dir1>" clojure.java.io/file file-seq (filter #(.isFile %)) sort))
  (def f2 (->> "<dir2>" clojure.java.io/file file-seq (filter #(.isFile %)) sort))
  (def r2 (->> f2 (mapcat #(scan-file ese %)) set))
  (def r1 (->> f1 (mapcat #(scan-file ese %)) set))
  (pprint (clojure.set/difference r2 r1))
  )



(comment
  "scan sessions timestamps"
  (require '[clj-time.format :as tf] '[clj-time.coerce :as tr])
  (def sessions (find-sessions nil))
  (def jira-ts-formatter (tf/formatters :date-time))
  (->> sessions (map (comp (partial map tr/to-date-time) vals :range :self)) pprint)
  )

(comment
  "remove specifc sessions"
  (require '[seazme.common.hbase :as hb])
  (require '[cbass :refer [new-connection store find-by scan delete pack-un-pack unpack without-ts get-table]])

  (def filter-2 (comp (partial = "scan") :command :self second))
  (def filter-1 (comp (partial = "jira") :kind :app second))

  (def ss2 (->> (hb/scan* "datahub:intake-sessions" :from (format "%011x" 1545852981000) :lazy? true)))
  (->> ss2 (filter filter-1) (filter filter-2) (filter filter-action) pprint)

  (def ssid "<tsx>\\<uuid>\\submit")
  (delete (hb/get-conn) "datahub:intake-sessions" ssid "self")
  (delete (hb/get-conn) "datahub:intake-sessions" ssid "app")
  )


(comment
  "find jira sessions"
  (def sessions (find-sessions nil))
  (def jira-sessions (->> sessions (filter (comp (partial = "jira") :kind :app))))
  (defn xx[e] [(->> e :meta :key) #_(->> e :meta :id) (->> e :meta :created d) (->> e :range vals (map d)) (:count e)])
  (defn d[x] (str (java.util.Date. x)))
  (binding [clojure.pprint/*print-right-margin* 302] (->> jira-sessions (take-last 60) (map (comp xx :self second)) pprint))
  )


(comment
  "cancel sessions"
  (defn cancel[skey]
    (let [sself (hb/find-by* "datahub:intake-sessions" skey :self)]
      (hb/store* "datahub:intake-sessions" skey :self
                 (assoc-in (assoc-in sself
                                     [:meta :action] "cancel")
                           [:description] "the session was manually canceled after initial submit due to JIRA API issue"))))
  )

(comment
  "misc"
  (def sessions (find-sessions nil))
  (->> sessions (map :self) (map #(quot (- (-> % :meta :created)  (Long/parseLong (-> % :meta :tsx) 16)) (:count %) )) pprint)
  )
