(comment
  "finding unique nil fields in JIRA"
  (require '[clj-time.format :as tf] '[clj-time.coerce :as tr])
  (use 'seazme.common.common 'seazme.common.datahub :reload)
  (def sessions (find-sessions nil))
  (def jira-sessions (->> sessions (filter (comp (partial = "jira") :kind :app))))
  (def jira-sessions-ids (->> jira-sessions (map (comp :id :meta :self))))
  (def recent-jira-sessions (->> jira-sessions (take-last 20) (map (comp :id :meta :self))))
  (def session-id (->> recent-jira-sessions last))

  (def jira-ts-formatter (tf/formatters :date-time))
  (def yymm-formatter (tf/formatter "YYMM"))
  (defn fmap [f m] (into (empty m) (for [[k v] m] [k (f v)])))
  (defn i4[x y] (assoc x (first y) ((partial merge-with +) (get x (first y) {}) (second y))))
  (defn fields4[ticket] (let [fields (->> ticket second :self :payload :fields)] [[(-> fields :project :key) (-> fields :issuetype :name) (->> fields :updated #_:created (tf/parse jira-ts-formatter) (tf/unparse yymm-formatter))] (fmap (comp {true 0 false 1} nil?) fields)]))
  (defn filter-zero-fileds[m] (->> m (remove #(->> % second zero?)) (into {})))
  (defn map-to-set[m] (->> m (map first) set))
  (defn nil-fields4a[session-id] (->> dist-bytes (map #(->> (get-data-entries-seq session-id %) (map fields4))) (apply concat) (reduce i4 {}) (fmap filter-zero-fileds)))
  (defn nil-fields4b[session-id] (->> dist-bytes (map #(->> (get-data-entries-seq session-id %) (map fields4) (fmap filter-zero-fileds))) (apply concat) (reduce i4 {})))
  (defn nil-tmp[session-id] (->> (get-data-entries-seq session-id "00") (map fields4) (reduce i4 {}) (fmap filter-zero-fileds)))

  (def t (nil-fields4 session-id))
  (def common-fields (apply clojure.set/intersection (->> t (fmap map-to-set) vals)))
  (def t2 (->> t (fmap #(apply dissoc % common-fields))))
  (def tt (->> recent-jira-sessions (take 3) (map nil-fields4) (appy merge))) ;;TODO add +
  (def common-fields (apply clojure.set/intersection (->> tt (fmap map-to-set) vals)))
  (def tt2 (->> tt (fmap #(apply dissoc % common-fields))))

  (->> tr (apply concat) (reduce i4 {}) (fmap filter-zero-fileds) (fmap #(apply dissoc % common-fields)) count)
  )


(comment
 "simple analytic on changes in tickets"

 (def ese #(vector (-> % :key) [(-> % :fields :updated) (-> % :fields :status :name)]))

 (defn ese2[t]
   (let [changelog (changelog-items t)
         changelog-pri (changelog-filter #{"priority"} changelog)
         esca (changelog-escalation escalated? changelog-pri)
         dees (changelog-escalation deescalated? changelog-pri)
         moved (count (changelog-filter #{"project"} changelog))
         reassigned (count (changelog-filter #{"assignee"} changelog))]
     [(t :key) [(-> t :key)
                (-> t :fields :project :name)
                (-> t :fields :status :name)
                (-> t :fields :issuetype :name)
                esca
                dees
                (count changelog-pri)
                moved
                reassigned]]))
 (def files (->> ".../x" clojure.java.io/file file-seq (filter #(.isFile %)) sort))
 (def x (reduce merge (map #(into {} (scan-file ese2 %)) files)))
 (->> x vals (map rest) frequencies (into []) (sort-by last) pprint)
 )


(common
 (def sessions (find-sessions nil))
 (def jira-sessions (->> sessions (filter (comp (partial = "jira") :kind :app))))
 (def small-jira-sessions (->> jira-sessions (filter (comp (partial > 200) :count :self second))))
 (def recent-jira-sessions (->> jira-sessions (take-last 5)))
 (def big-jira-sessions (->> jira-sessions (filter (comp (partial < 20000) :count :self second))))
 (defn essential-per-session[session]
   (let[sid (-> session :self :meta :id)
        {:keys [from to]} (-> session :self :range)
        essef #(vector (->> % second :self :payload :key) (->> % second :self :payload :id) (->> % second :self :payload :fields :status :name) (->> % second :self :payload :fields :updated))
        essef2 #(let [x (essef %)] (prn x ) x)]
     (assert (not (nil? sid)))
     (vector sid from to (->> dist-bytes (pmap #(->> (get-data-entries-seq sid %) (map essef))) (mapcat identity) doall))))

 (def xx (essential-per-session (->> big-jira-sessions last second)))

 (->> xx last (group-by first) (map second) (filter #(> (count %) 1)) (map ffirst) sort pprint)
 )
