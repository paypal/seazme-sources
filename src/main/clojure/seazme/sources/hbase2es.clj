(ns seazme.sources.hbase2es
  (:require [clojure.tools.logging :as log]
            [seazme.common.es :as ces])
  (:use seazme.sources.common
        seazme.common.common
        seazme.common.datahub
        seazme.common.notif))

(defn- update-datasources[conn session]
  (let [session-tsx (-> session :self :meta :tsx)
        datasource-record {:current_status "working"
                           :business_unit (-> session :app :bu)
                           :owners "TBD"
                           :last_updated_time (-> session :self :range :to (quot 1000))
                           :tag "N/A" :name (-> session :app :kind)
                           :id (-> session :app :meta :id)
                           :notes (str (-> session :app :description) ", from:" session-tsx)}]
    (try
      (ces/put-doc! conn "datasources" "datasources" datasource-record)
      (catch Exception e (do
                           (prn "ERROR3 put-doc!" e);;TODO check logs for ERROR3 - do not catch again
                           (try
                             (ces/put-doc! conn "datasources" "datasources" datasource-record)
                             (catch Exception e
                               (prn "ERROR4 put-doc!" e))))))))

(defn- update-doc[conn indx expl]
  (let [kind (-> expl second :app :kind)
        bu (-> expl second :app :bu)
        instance (-> expl second :app :instance)
        base-url (-> expl second :app :base-url)
        parsed-topic ((get kind2parse kind default-parse-topic) kind bu instance base-url (-> expl second :self :payload))]
    (if parsed-topic
      (try
        (ces/put-doc! conn indx kind parsed-topic)
        (catch Exception e (do
                             (prn "ERROR1 put-doc!" e expl);;TODO check logs for ERROR2 - do not catch again
                             (try
                               (ces/put-doc! conn indx kind parsed-topic)
                               (catch Exception e
                                 (prn "ERROR2 put-doc!" e expl))))))
      {:kind kind :instance instance :bu bu :created nil};;TODO throw&exit if no conversion
      )))


(defn- update-es[update-doc-fn update-datasources-fn session]
  (let [session-id (-> session :self :meta :id)
        session-tsx (-> session :self :meta :tsx)
        res (->> dist-bytes
                  (pmapr 4 #(->> % (get-data-entries-seq session-id) (map update-doc-fn) doall))
                  (mapcat identity)
                  (map #(dissoc % :_id :_shards :_version))
                  frequencies)]
    (update-datasources-fn session)
    (println "\tupdated:" res)
    (->> res vals (reduce +))))

(defn- scan-action-es[indx-exists? indx-create upload prefix ts prev-ts app session]
  (let [indx (app2index2 prefix app ts)]
    (when (indx-exists? indx)
          (throw (Exception. (str "index" " \"" indx "\" " "already exists!"))))
    (indx-create indx (-> app :kind))
    (if (nil? prev-ts) ;;TODO add ES cluster, prefix, machine hosting batch etc ..
      (send-email "Seazme new index notification" (format "Hello Admin,<br><br>A new index was created %s.<br><br>Cheers,<br>Seazme" indx))
      (send-email "Seazme index change notification" (format "Hello Admin,<br><br>A new index was created %s and it might replace existing %s.<br><br>Cheers,<br>Seazme" indx (app2index2 prefix app prev-ts))))
    (upload indx session)))

(defn- update-action-es[indx-exists? indx-create upload prefix ts prev-ts app session]
  (let [indx (app2index2 prefix app prev-ts)]
    (when-not (indx-exists? indx)
              (throw (Exception. (str "index" " \"" indx "\" " "does not exist!"))))
    (upload indx session)))

(defn process-sessions![{:keys [prefix]} action es-conn]
  (let [indx-exists? (fn [indx] (ces/exists? es-conn indx))
        indx-create (fn [indx knd] (println "index " indx " has been created") (ces/reinit! {:index indx :kind knd} es-conn))
        upload (fn[indx session] (update-es (partial update-doc es-conn indx) (partial update-datasources es-conn) session))
        scan-action (partial scan-action-es indx-exists? indx-create upload prefix)
        update-action (partial update-action-es indx-exists? indx-create upload prefix)]
    (run-update-log! prefix action scan-action update-action)))
