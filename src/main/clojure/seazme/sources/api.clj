;;TODO move to LIB
(ns seazme.sources.api
  (:require [clj-http.client :as c]
            [clojure.data.json :as json]))

(def api-map {:get c/get :post c/post})

;;TODO combine two below and make it accept single auth input
(defn mk-conf-api
  ;;conf-url-base "https://*/confluence/rest/api"
  ;;cookie
  ;;TODO add timer for heart beat and remove session
  [op conf-url-base cookie timeout] ;;TODO add throw exeptions param
  (let [default-params {:insecure? true :socket-timeout timeout :conn-timeout timeout :debug false}
        session-cooookie cookie
        opf (op api-map)]
    (fn run-api
      ([rest-path body] ;;rest-path body {:key "value"}
       (let [client-f-params-base (assoc default-params :throw-exceptions true)
             client-f-params (if body (assoc client-f-params-base :content-type :json :body (json/write-str body)) client-f-params-base)
             client-f (fn[] (opf (str conf-url-base rest-path) (assoc client-f-params :cookies {"JSESSIONID" {:value session-cooookie}})))
             res (client-f)]
         res))
      ([rest-path] (run-api rest-path nil)))))

(defn mk-conf-api-auth ;;for now copy paste of main method with some tweeks
  ;;conf-url-base "https://*/confluence/rest/api"
  ;;cookie
  ;;TODO add timer for heart beat and remove session
  [op conf-url-base basic-auth timeout] ;;TODO add throw exeptions param
  (let [default-params {:insecure? true :basic-auth basic-auth :socket-timeout timeout :conn-timeout timeout :debug false}
        opf (op api-map)]
    (fn run-api
      ([rest-path body] ;;rest-path body {:key "value"}
       (let [client-f-params-base (assoc default-params :throw-exceptions true)
             client-f-params (if body (assoc client-f-params-base :content-type :json :body (json/write-str body)) client-f-params-base)
             client-f (fn[] (opf (str conf-url-base rest-path) client-f-params))
             res (client-f)]
         res))
      ([rest-path] (run-api rest-path nil)))))

(defn api-follow-link [getfn url]
  (let [ret (getfn (str url "&limit=100")) ;; or ?
        status (:status ret)
        body (json/read-str (:body ret) :key-fn keyword)
        results (:results body)
        next (get-in body [:_links :next]) ;;add warning when limit == size and there is no next, something unlikely to happen
        _ (println url status next #_(dissoc ret :body))
        _ (Thread/sleep 100)]
    (when (= 200 status) ;;TODO 200 still returned if no authenticated
      (if next
        (concat
         (api-follow-link getfn next) results)
        results))))

(defn api-single [getfn url]
  (let [ret (getfn url)
        status (:status ret)
        body (json/read-str (:body ret) :key-fn keyword)]
    (when (= 200 status)
      body)))

