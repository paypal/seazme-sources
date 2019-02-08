(ns seazme.sources.jira-api
  (:require [clj-http.client :as c]
            [clojure.data.json :as json])
            )

(defn- remove-nilvals[m] (into {} (remove (comp nil? second) m)))

(def api-map {:get c/get :post c/post})

(defn parse-int [s]
  (Integer/parseInt (re-find #"\A-?\d+" s)))

(defn mk-jira-api
  ;;jira-url-base "https://*/jira/rest"
  ;;credens {:username "username" :password "password"}
  ;;TODO add timer for heart beat and remove session
  ;;cannot use {:basic-auth acct} - due to way JIRA handles sessions
  [op re-auth-f jira-url-base credens timeout debug]
  (let [default-params {:insecure? true :socket-timeout timeout :conn-timeout timeout}
        session-cooookie (atom nil)
        opf (op api-map);;TODO remove SESSION header
        auth-f #(let [res (c/post (str jira-url-base "/auth/1/session") (assoc default-params :content-type :json :body (json/write-str credens)))
                      _ (when debug (prn "DEBUG0:" res))
                      cookie (-> res :body (json/read-str :key-fn keyword) :session :value)]
                  (reset! session-cooookie cookie))]
    (fn run-api
      ([rest-path body] ;;rest-path body {:key "value"}
       ;;auth first time
       (if-not @session-cooookie
         (auth-f))
       (let [client-f-params-base (assoc default-params :throw-exceptions false)
             client-f-params (if body (assoc client-f-params-base :content-type :json :body (json/write-str body)) client-f-params-base)
             client-f (fn[] (opf (str jira-url-base "/" rest-path) (assoc client-f-params :cookies {"JSESSIONID" {:value @session-cooookie}})))
             _ (when debug (prn "DEBUG1:" (str jira-url-base "/" rest-path) (assoc client-f-params :cookies {"JSESSIONID" {:value @session-cooookie}})))
             res (client-f)]
         ;;handle expired session
         (if (re-auth-f res)
           (do (auth-f)
               (client-f))
           res)))
      ([rest-path] (run-api rest-path nil))
      ([] debug))))
;;TODO close session, if rest path is nil

(def timeout (* 5 60 1000))

;;TODO follow up with atlasian
;;401 indicates a need of authentication but not documented for HTTP GET issue
(defn pja-rea-401 [res] (= 401 (:status res)))
;;400 indicates a need of authentication for HTTP POST search however recently, it was returning "default" project
(defn pja-rea-400 [res] (= 400 (:status res)))

(defn mk-pja-api[op re-auth-f rest-url-base acct debug] (mk-jira-api op re-auth-f rest-url-base (zipmap [:username :password] acct) timeout debug))

(defn mk-pja-issue[rest-url-base acct debug & opt]
  (let [f (mk-pja-api :get pja-rea-401 rest-url-base acct debug)]
    (fn [id] (f (apply str "api/latest/issue/" id opt)))))
(defn mk-pja-issue-full[rest-url-base acct] (mk-pja-issue rest-url-base acct "?expand=changelog,transitions"))

(defn mk-pja-search-api[rest-url-base acct debug]
  (let [f (mk-pja-api :post pja-rea-400 rest-url-base acct debug)]
    (fn
      ([req] (f (apply str "api/2/search") req))
      ([] (f)))))

(defn pja-search-limited [f jql fields limit]
  (let [req {:jql jql :fields fields :startAt 0 :maxResults limit}
        r (f req)
        rr (json/read-str (:body r) :key-fn keyword)]
    (:issues rr)))

(defn pja-search [f jql {:keys [fields expand] :or {fields ["dummy"] expand nil}} cb]
  (let [step 100
        req (remove-nilvals {:jql jql :fields fields :expand expand :maxResults step})
        debug (f)]
    (loop [res []
           startAt 0
           total -1]
      (let [_ (when debug (prn "DEBUG2" (assoc req :startAt startAt)))
            r (f (assoc req :startAt startAt))
            rr (json/read-str (:body r) :key-fn keyword)
            new-total (:total rr)
            _ (when debug (prn "DEBUG3" total new-total startAt (dissoc r :body) (dissoc rr :issues :names :schema) (count (:issues rr)) (count (:names rr))))
            new-res (->> rr :issues (map cb) doall)]
        (assert (<= total (+ new-total step)) (str "false 200:" total "," new-total "," (pr-str jql)))
        (if (> startAt (:total rr))
          res
          (recur (concat res new-res) (+ step startAt -10) (if (neg? total) new-total total)))))))

(defn pja-search-full [f jql cb]
  (pja-search f jql {:fields ["*all"] :expand ["names","schema","transitions","comment","editmeta","changelog"]} cb))

#_(defn pja-last [f project-or-project-key]
  (let [c (pja-search-limited f (str "project = " project-or-project-key " " "ORDER BY created DESC") ["id" "key"] 2) ;;TODO ["id" "key"] is not correct here, any non empty list of fields will get :id and :key. JIRA API spec does not describe "fields" well
        r (re-find #"([A-Z]+)-([0-9]+)" (:key (first c)))]
    {:key (r 0) :project-key (r 1) :issue-number (parse-int (r 2))}))
