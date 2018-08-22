(ns seazme.common.es
  (:require
   [clojurewerkz.elastisch.rest :as esr]
   [clojurewerkz.elastisch.rest.index :as esi]
   [clojurewerkz.elastisch.rest.document :as esd])
  )

(defn mk-connection[profile & {:keys [debug debug-body] :or {debug false debug-body false}}]
  (esr/connect (profile :host) {:socket-timeout 60000 :conn-timeout 60000 :insecure? true :basic-auth (profile :basic-auth) :debug debug :debug-body debug-body}))


(def sna {:type "string" :index "not_analyzed"})
(defn reinit![{:keys [index kind]} conn]
  (let [mapping-types {kind
                       {:properties
                        {:url sna
                         :kind-name sna
                         :bu-name sna
                         :instance-name sna
                         :level0 sna
                         :level1 sna
                         :parent-id sna
                         :last-author sna
                         :last-ts sna
                         :text {:type "string" :analyzer "snowball"}
                         :text-size {:type "integer"}
                         }}}]
    (esi/create conn index {:mappings mapping-types})))

(defn reinit-datasources![conn]
  (let [indx "datasources"
        mapping-types {"datasources"
                       {:properties
                        {:current_status sna
                         :name sna
                         :owners sna
                         :business_unit sna
                         :last_updated_time {:type "integer"}
                         :tag sna
                         :notes sna
                         }}}]
    [(esi/delete conn indx)
     (esi/create conn indx {:mappings mapping-types})]))

;;TODO can we create two same docs? test it
(defn put-doc![conn indx ttype doc]
  (esd/put conn indx ttype (:id doc) doc)) ;;TODO can we create two same docs? test it

(defn exists?[conn indx]
  (esi/exists? conn indx))
