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

;;TODO add counter
(defn- update-es[update-doc-fn update-datasources-fn session]
  (let [session-id (-> session :self :meta :id)
        session-tsx (-> session :self :meta :tsx)
        res (->> dist-bytes
                  (pmapr 4 #(->> % (get-data-entries-seq session-id) (map update-doc-fn) doall))
                  (mapcat identity)
                  (map #(dissoc % :_id :_shards :_version))
                  frequencies)]
    (update-datasources-fn session)
    (println "updated:" session-tsx "for session-id" session-id ":" res)
    res))

(defn- process-session![prefix indx-exists? indx-create upload [old-tsx old-created a2i a2u] session-kv]
  (let [session (-> session-kv second)
        self (-> session :self)
        app (-> session :app)
        tsx (-> self :meta :tsx)
        ts (-> self :meta :created)
        created (-> self :meta :created)
        u (select-keys app [:instance :bu :kind])]
    (println "processing:" u ts tsx)
    (if (pos? (compare tsx (a2u u))) ;;tsx in session is newer than from update-log ;;TODO critical for UT
      (let [[indx new-a2i] (if (-> self :command (= "scan"))
                             (let [old-ts (a2i u)
                                   indx (app2index2 prefix app ts)]
                               (when (indx-exists? indx)
                                 (throw (Exception. (str "index" " \"" indx "\" " "already exists!"))))
                               (indx-create indx (-> app :kind))
                               (if (nil? old-ts) ;;TODO add ES cluster, prefix, machine hosting batch etc ..
                                 (send-email "Seazme new index notification" (format "Hello Admin,<br><br>A new index was created %s.<br><br>Cheers,<br>Seazme" indx))
                                 (send-email "Seazme index change notification" (format "Hello Admin,<br><br>A new index was created %s and it might replace existing %s.<br><br>Cheers,<br>Seazme" indx (app2index2 prefix app old-ts))))
                               [indx (assoc a2i u ts)])
                             (let [indx (app2index2 prefix app (a2i u))]
                               (when-not (indx-exists? indx)
                                 (throw (Exception. (str "index" " \"" indx "\" " "does not exist!"))))
                               [indx a2i]))]
        (upload indx session)
        [tsx created new-a2i (assoc a2u u tsx)])
      [tsx created a2i a2u])))

(defn process-sessions![{:keys [prefix]} es-conn]
  (let [indx-exists? (fn [indx] (ces/exists? es-conn indx))
        indx-create(fn [indx knd] (println "index " indx " has been created") (ces/reinit! {:index indx :kind knd} es-conn))
        upload (fn[indx session] (update-es (partial update-doc es-conn indx) (partial update-datasources es-conn) session))]
    (process-update-log! (partial process-session! prefix indx-exists? indx-create upload) prefix)))

(comment
  "run it manually"
  (use 'seazme.sources.common :reload)
  (require ' [seazme.common.hbase :as hb])
  (use 'seazme.sources.hbase2es :reload)
  (require ' [seazme.common.es :as ces])

  (def apps (hb/scan* "datahub:apps"))
  (def min-tsx (->> apps apps2map vals sort first)) ;; or it with snapshot-update-log before
  (def max-tsx (->> apps apps2map vals sort last)) ;; or rather at the end of reduce
  (def sessions-kv (find-sessions min-tsx))
  (def a2u (apps2map apps)) ;; with last read union

  (def e-p-s (ces/mk-connection (config/config :elasticsearch-prod-standby)))
  (proc-sess-save e-p-s "pref4" a2u sessions-kv)
  )

(comment
  "finding unique nil fields in JIRA"
  (require ' [seazme.common.hbase :as hb])
  (use 'seazme.sources.common :reload)
  (def sessions (find-sessions nil))
  (def session-id (->> sessions (map (comp :id :meta :self second)) last))
  (def sample-tickets (->> dist-bytes (take 30) (pmap (partial get-data-entries-seq session-id)) (mapcat identity)))
  (def sample-fields (->> sample-tickets (map #(->> % second :self :payload :fields))))
  (defn filter-nil-fileds[m] (->> m (filter #(->> % second nil?)) (map first) set))
  (count sample-tickets)
  (->> sample-fields (map filter-nil-fileds) (map count) frequencies)
  (->> sample-fields (map filter-nil-fileds) (reduce clojure.set/intersection) count)
  )


(comment
  "delete bad prefix"
  (def prefix "badprefix")
  (def to-del (->> (hb/scan* "datahub:snapshot-update-log" :starts-with prefix :lazy? true) (map (comp name first))))
  (->> to-del (map (partial delete (hb/get-conn) "datahub:snapshot-update-log")) dorun)
  )

(comment
  "delete bad indices"
  (->> (esi/get-settings e-p-s) keys (map name) (map (partial esi/delete e-p-s)) doall frequencies)
  )
