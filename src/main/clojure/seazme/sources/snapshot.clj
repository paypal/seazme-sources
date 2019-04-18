(ns seazme.sources.snapshot
  (:require [seazme.common.hbase :as hb]
            [cbass.tools :refer [to-bytes]]
            [cbass :refer [pack-un-pack]][taoensso.nippy :as n]);;this is hack waiting for fix: https://github.com/tolitius/cbass/issues/9
  (:use seazme.sources.common
        seazme.common.datahub
        seazme.common.common))

(defn- transform-value[f v] (if (vector? v) (clojure.string/join "," (map f v)) (str (f v))))

;;When the key is a map/hash, it provides name mapping from to. It has to have one entry
;;When the key is a keyword (thing with : in front), the name of the field will remain the same
;;The value is a vector/array and if it is empty, e.i [], the actual value will be taken from the original JSON
;;If it is not empty, it will indicate a “path” to the element: e.g. [“name”] is a common path
;;Keywords (things with : in front) are used instead of strings for better input validation.
;;Unlike in JSON, keys in maps/hashes could be a composite data structure, not only strings
(defn apply-field-mapping[m d]
  (let [nm (into [] (for [[k v] m] (if (map? k) [(ffirst k) (second (first k)) v] [k k v])))];;k should have one pair
    (mapcat
     (fn [[ok fk v]]
       (let [dv (get d ok "")]
         (condp #(%1 %2) v
           map? (apply-field-mapping v dv) ;;fk disregarded
           vector? [[(name fk) (transform-value #(get-in % v) dv)]]
           :else (assert false "wrong type"))))
     nm)))

(def field-mapping (-> "mapping.edn" slurp read-string))

(defn- update-hbase![expl]
  (let [jts (jts-now)
        payload (-> expl second :self :payload)
        hbase-payload (into {} (apply-field-mapping field-mapping payload))
        document-key (clojure.string/reverse (:id payload))
        document-meta {
                       ;;:session {:id session-id :key session-key :tsx session-tsx}
                       :key document-key
                       :created jts}]
    (println "posting:" (:key payload) (:id payload) (-> payload :fields :updated))
    (hb/store* "datahub:snapshot-data" document-key :raw {:payload payload :type "document" :meta document-meta})
    (pack-un-pack {:p #(to-bytes %)})
    (hb/store** "datahub:snapshot-data" document-key "jirappmain" hbase-payload)
    (pack-un-pack {:p n/freeze})))

(defn update-snapshot![session]
  (let [session-id (-> session :self :meta :id)
        session-tsx (-> session :self :meta :tsx)
        session-count (-> session :self :count)
        _ (println "updating:" session-count "entries for" session-tsx "and session-id" session-id "...")
        real-cnt (->> dist-bytes
                 (map #(->> % (get-data-entries-seq session-id) (map update-hbase!) doall))
                 (mapcat identity)
                 count)]
    (println "updated:" session-count "entries for" session-tsx "and session-id" session-id " with " real-cnt)
    real-cnt
    ))

(defn- process-session![prefix upload [old-tsx old-created a2i a2u] session-kv]
  (let [session (-> session-kv second)
        self (-> session :self)
        app (-> session :app)
        tsx (-> self :meta :tsx)
        ts (-> self :meta :created)
        created (-> self :meta :created)
        u (select-keys app [:instance :bu :kind])]
    (println "processing:" prefix u ts tsx)
    (if (and (pos? (compare tsx (a2u u))) (= "e7228880-27a4-11e8-8178-4f6e83cbd595" (-> app :meta :key))) ;;tsx in session is newer than from update-log ;;TODO critical for UT
      (let [cnt (upload session)]
        [tsx created (assoc a2i u ts) (assoc a2u u tsx) cnt])
      [tsx created a2i a2u nil])))

(defn process-sessions![{:keys [prefix]}]
  (process-update-log! (partial process-session! prefix update-snapshot!) prefix))
