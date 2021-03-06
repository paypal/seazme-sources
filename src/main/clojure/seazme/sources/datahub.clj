(ns seazme.sources.datahub
  (:require
   [clojure.data.json :as json]
   [clj-http.client :as chc]
   [seazme.sources.twiki :as t]
   [seazme.sources.confluence :as c]
   [seazme.sources.jira-api :as jira-api]
   [seazme.sources.jira :as j]
   [seazme.sources.snow :as s]
   [clj-time.format :as tf] [clj-time.core :as tr] [clj-time.coerce :as te]
   [clojure.tools.logging :as log])
  (:use seazme.common.common))


;;TODO replace conf->confluence everywhere
;;TODO check datahub status prior starting sessions

;;
;; common
;;
(def continue-default 40) ;;depends on hub's time-span and find-periods
(defn make-sure[]
  ;;TODO automate this
  (println "Please delete a few last entries/days from cache dir, as they contain unnecessary data")
  (println "Warning: it might take a while to see results as all entries in the cache have to be verified"))
(defn- wrap-with-counter[f] (let [counter (atom 0)]
                              [counter (fn[& args] (swap! counter inc) (apply f args))]))
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

;;
;; Twiki
;;
(defn twiki-scan![{:keys [app-id index kind path]} d {:keys [path]}]
  #_(prn "DEBUG" index kind d s)
  (println "STARTING twiki-scan:"app-id index kind path(str "@"(tr/now)))
  (let [p (mk-datahub-post-api d)
        {:keys [body status]} (p (format "intake-sessions?app-id=%s&description=twiki full scan&command=scan" app-id) nil)
        session-id (:key body)
        [counter cb] (wrap-with-counter #(p (format "intake-sessions/%s/document" session-id) (json/write-str %)))
        ret1 (->>
         path
         t/find-topics
         #_(take 10)
         ;;TODO do partitions
         (remove nil?)
         (map t/read-topic!)
         (remove nil?)
         (map cb)
         frequencies)
        ret2 (p (format "intake-sessions/%s/submit?count=%d" session-id @counter) nil)
        ]
    (println "SUCCEEDED:" (pr-str ret1) (pr-str ret2)(str "@"(tr/now)))
    )
  )

;;
;; Confluence
;;
(defn confluence-scan![{:keys [app-id index kind]} d s]
  #_(prn "DEBUG" index kind d s)
  (println "STARTING confluence-scan:"app-id index kind(str "@"(tr/now)))
  (let [p (mk-datahub-post-api d)
        {:keys [body status]} (p (format "intake-sessions?app-id=%s&description=Confluence full scan&command=scan" app-id) nil)
        session-id (:key body)
        [counter cb] (wrap-with-counter #(p (format "intake-sessions/%s/document" session-id) (json/write-str %)))
        ret1 (->>
              s
              c/find-spaces
              ;;(drop 1) (take 1);;TODO
              (map (partial c/save-space-from-search2 s cb))
              flatten
              frequencies)
        ret2 (p (format "intake-sessions/%s/submit?count=%d" session-id @counter) nil)
        ]
    (println "SUCCEEDED:" (pr-str ret1) (pr-str ret2)(str "@"(tr/now)))
    )
  )
(defn confluence-update![{:keys [app-id index kind]} d s continue]
  #_(prn "DEBUG" index kind d s continue)
  (println "STARTING confluence-update:"app-id index kind(str "@"(tr/now)))
  (let [p (mk-datahub-post-api d)]
    (loop [limit continue-default]
      (when (pos? limit)
        (let [{:keys [body status]} (p (format "intake-sessions?app-id=%s&description=Confluence update&command=update" app-id) nil)]
          (condp = status
            200 (let [session-id (:key body)
                      {{:keys [from to]} :range} body
                      [counter cb] (wrap-with-counter #(p (format "intake-sessions/%s/document" session-id) (json/write-str %)))
                      ret1 (->> (c/pull-confl-incr2 s cb from to) frequencies)
                      ret2 (p (format "intake-sessions/%s/submit?count=%d" session-id @counter) nil)]
                  (println "SUCCEEDED:" (jts-to-str from) (jts-to-str to) status (pr-str body) (pr-str ret1) (pr-str ret2)(str "@"(tr/now)))
                  (when continue (recur (dec limit))))
            202 (println "FINISHED:" status (pr-str body)(str "@"(tr/now)))
            (log/error "FAILED" status body)))))))

;;
;; JIRA
;;

(def scanned-jira-keys (atom {}))
(defn- to-datahub[p payload]
  (let [jira-key (-> payload :key)
        jira-id (-> payload :id)
        updated (-> payload :fields :updated)]
    (locking (.intern jira-key)
      (let [old-updated (@scanned-jira-keys jira-key)]
        (if (neg? (compare old-updated updated))
          (do
            (swap! scanned-jira-keys assoc jira-key updated)
            (sync-println "\tposting:" jira-key jira-id updated old-updated)
            (and p (p payload)))
          (sync-println "\tskipping:" jira-key jira-id updated old-updated))))))

(defn jira-scan![context d s]
  #_(prn "DEBUG" context d s)
  (println "STARTING jira-scan:"context(str "@"(tr/now)))
  (make-sure)
  (let [{:keys [app-id index kind]} context
        pf (get s :parallel-factor 1)
        p (mk-datahub-post-api d)
        {:keys [body status]} (p (format "intake-sessions?app-id=%s&description=JIRA full scan&command=scan" app-id) nil)
        session-id (:key body)
        pja-search-api (jira-api/mk-pja-search-api (:url s) (:credentials s) (:debug s))
        [counter cb] (wrap-with-counter (partial to-datahub #(p (format "intake-sessions/%s/document" session-id) (json/write-str %))))
        ret1 (->>
              (j/find-periods)
              (pmapr pf (partial j/upload-period context (:cache s) false pja-search-api cb))
              flatten
              frequencies)
        ret2 (p (format "intake-sessions/%s/submit?count=%d" session-id @counter) nil)]
    (println "SUCCEEDED:" (pr-str ret1) (pr-str ret2)(str "@"(tr/now)))
    ))
(defn jira-patch![context d s jql]
  #_(prn "DEBUG" context d s jql)
  (println "STARTING jira-patch:"context(str "@"(tr/now)))
  (println "WARNINIG: please suspend update due to slight risk of overwriting issues")
  (let [{:keys [app-id index kind]} context
        p (mk-datahub-post-api d)
        {:keys [body status]} (p (format "intake-sessions?app-id=%s&description=JIRA patch&command=patch" app-id) nil);;TODO for parameters
        #_ (prn "DEBUG" body)
        session-id (:key body)
        pja-search-api (jira-api/mk-pja-search-api (:url s) (:credentials s) (:debug s))
        [counter cb] (wrap-with-counter (partial to-datahub #(p (format "intake-sessions/%s/document" session-id) (json/write-str %))))
        ret1 (j/upload-by-jql context pja-search-api cb jql)
        ret2 (p (format "intake-sessions/%s/submit?count=%d" session-id @counter) nil)]
    (println "SUCCEEDED:" (pr-str ret1) (pr-str ret2)(str "@"(tr/now)))
    ))
(defn jira-update![context d s continue]
  #_(prn "DEBUG" context d s continue)
  (println "STARTING jira-update:"context(str "@"(tr/now)))
  (let [{:keys [app-id index kind]} context
        p (mk-datahub-post-api d)
        pja-search-api (jira-api/mk-pja-search-api (:url s) (:credentials s) (:debug s))]
    (loop [limit continue-default]
      (when (pos? limit)
          (let [{:keys [body status]} (p (format "intake-sessions?app-id=%s&description=JIRA update&command=update" app-id) nil)]
            (condp = status
              200 (let [session-id (:key body)
                        {{:keys [from to]} :range} body
                        [counter cb] (wrap-with-counter (partial to-datahub #(p (format "intake-sessions/%s/document" session-id) (json/write-str %))))
                        ret1 (j/upload-period context (:cache s) false pja-search-api cb (list from to))
                        ret2 (p (format "intake-sessions/%s/submit?count=%d" session-id @counter) nil)]
                    (println "SUCCEEDED:" (jts-to-str from) (jts-to-str to) status (pr-str body) (pr-str ret1) (pr-str ret2)(str "@"(tr/now)))
                    (when continue (recur (dec limit))))
              202 (println "FINISHED:" status (pr-str body)(str "@"(tr/now)))
              (log/error "FAILED" status body)))))))

(defn jira-scan-to-cache![context s]
  #_(prn "DEBUG" context s)
  (println "STARTING jira-scan-to-cache:"context(str "@"(tr/now)))
  (make-sure)
  (let [pja-search-api (jira-api/mk-pja-search-api (:url s) (:credentials s) (:debug s))
        pf (get s :parallel-factor 1)]
    (->>
     (j/find-periods)
     (pmapr pf (partial j/upload-period context true true pja-search-api #(sync-println "\tposting:" (:key %) (:id %) (-> % :fields :updated))))
     flatten
     frequencies)
    ))
(defn jira-patch-to-cache![context d s jql]
  #_(prn "DEBUG" context d s jql)
  (println "STARTING jira-patch-to-cache:"context(str "@"(tr/now)))
  (let [pja-search-api (jira-api/mk-pja-search-api (:url s) (:credentials s) (:debug s))]
    (->>
     (j/upload-by-jql context pja-search-api (constantly "done") jql)
     frequencies)
    ))

;;
;; SNOW
;;
(defn- print-and-pass[a] (println (frequencies a)) a)
(defn snow-scan![{:keys [index kind]} d s]
  (let [p (mk-datahub-post-api d)
        api (:basic-auth s)
        in-se (p "intake-sessions?app-id=4&description=this is snow test&command=scan" nil);;TODO appid from config
        cb #(p (format "intake-sessions/%s/document" (:key in-se)) (json/write-str %))]
    (->>
     (s/find-periods)
     (map (partial s/upload-period (s/mk-snow-api api) cb))
     (map print-and-pass)
     flatten
     frequencies)
    )
  )
