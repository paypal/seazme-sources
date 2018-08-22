(ns seazme.sources.es2
  (:require [seazme.common.hbase :as hb])
  (:use seazme.sources.common
        seazme.common.common
        seazme.common.datahub
        seazme.sources.es)
  )

(let[file-name #(str % "-last-state.edn")]
  (defn read-last-state[prefix]
    (try
      (from-edn (file-name prefix))
      (catch Exception e (throw (Exception. (str "please (re)create file:" (file-name prefix) " with last state or nil if deployed for the first time"))))))
  (defn write-last-state[prefix state]
    (to-edn (file-name prefix) state)))

(defn update-es[conn prefix expl]
  (let [kind (-> expl second :app :kind)
        bu (-> expl second :app :bu)
        instance (-> expl second :app :instance)
        base-url (-> expl second :app :base-url)
        parsed-topic ((get kind2parse kind default-parse-topic) kind bu instance base-url (-> expl second :self :payload))]
    (if parsed-topic
      (try
        (put-doc! conn (app2index prefix (-> expl second :app)) kind parsed-topic)
        (catch Exception e (do
                             (prn "ERROR1 put-doc!" e expl)
                             (try
                               (put-doc! conn (app2index prefix (-> expl second :app)) kind parsed-topic)
                               (catch Exception e
                                 (prn "ERROR2 put-doc!" e expl)
                                      ))
                             )))
      {:kind kind :instance instance :bu bu :created nil})))

;;boot.user=> (time (->> dist-bytes (pmap (partial get-data-entries-seq session-id)) (mapcat identity) (take 1000) (map (partial update-es esc "test")) (map :created) frequencies))
;;boot.user=> (def session-ids (->> dist-bytes (pmap (partial get-data-entries-seq session-id)) (mapcat identity) (map (partial update-es esc "test"))))
;;boot.user=> (->> session-ids (map #(dissoc % :_id :_shards :_version)) frequencies pprint)
;;{{:_index "test-jira-pp-main", :_type "jira", :created false} 2202}
;;nil

;;TODO add counter, add tsx to ES/datasources, show all sessions before updating
(defn- remove-noise[e] (dissoc e :_id :_shards :_version))
(defn hbase-update! [{:keys [prefix]} conn]
  (let [last-update (read-last-state prefix)
        update-es-fn (partial update-es conn prefix)
        _ (println "Last state:" last-update)
        sessions (find-sessions last-update)
        upload-one-session-fn (fn[session-entry-kv]
                                (let [session-id (-> session-entry-kv second :self :meta :id)
                                      session-tsx (-> session-entry-kv second :self :meta :tsx)
                                      datasource-record (let [e (-> session-entry-kv second)]
                                                          {:current_status "working"
                                                           :business_unit (-> e :app :bu)
                                                           :owners "TBD"
                                                           :last_updated_time (-> e :self :range :to (quot 1000))
                                                           :tag "N/A" :name (-> e :app :kind)
                                                           :id (-> e :app :meta :id)
                                                           :notes (str (-> e :app :description) ", from:" session-tsx)})
                                      res1 (->> dist-bytes
                                               (pmapr 4 #(->> % (get-data-entries-seq session-id) (map update-es-fn) doall))
                                               (mapcat identity)
                                               (map remove-noise)
                                               doall)
                                      res2 (put-doc! conn  "datasources" "datasources" datasource-record)];;TODO wrap with try like above
                                  (write-last-state prefix session-tsx)
                                  (println "updated:" session-tsx "for session-id" session-id)
                                  res1))]
    (->> sessions (mapcat upload-one-session-fn) frequencies println)))
