(ns seazme.sources.datahub
  (:require
   [clojure.data.json :as json]
   [clj-http.client :as chc]
   [clj-time.format :as tf] [clj-time.core :as tr] [clj-time.coerce :as te]
   ))


;;TODO replace conf->confluence everywhere
;;TODO check datahub status prior starting sessions

;;
;; common
;;
(defn make-sure[] (prn "Please make sure that a few last entries/days from cache are manually deleted"));;TODO automate this
(defn- wrap-with-counter[f] (let [counter (atom 0)]
                              [counter (fn[& args] (swap! counter inc) (apply f args))]))
(defn- print-and-pass[a] (println (frequencies a)) a)
(defn- jts-to-str[jts] (->> jts te/from-long (tf/unparse (tf/formatters :mysql))))

;;TODO move insecure to config
;;combine get and post

;;TODO handle 413 > than client_max_body_size 64M;
(defn mk-datahub-post-api[dhs-config]
  (let [url (str (:host dhs-config) "/v1/datahub/")
        options {:insecure? true
                 :throw-exceptions true
                 :basic-auth (:basic-auth dhs-config)
                 :headers {"apikey" "testuser"}
                 :as :json}]
    (fn [rest-path body]
      (select-keys (chc/post (str url rest-path)
                       (if body (merge options {:content-type :json :body body}) options))
             [:body :status]
             ))))

#_(chc/post "https://*/v1/datahub/intake-sessions/20171110003823\\7506ad50-c5af-11e7-9dee-4f6e83cbd595/document"
          {:content-type :json
           :body (json/write-str content)
           :insecure? true
           :throw-exceptions false
           :basic-auth ["*" "*"]
           :as :json})
(defn mk-datahub-get-api[dhs-config]
  (let [url (str (:host dhs-config) "/v1/datahub/")
        options {:insecure? true
                 :throw-exceptions true
                 :basic-auth (:basic-auth dhs-config)}]
    (fn [rest-path]
      (:body (chc/get (str url rest-path) options)))))
