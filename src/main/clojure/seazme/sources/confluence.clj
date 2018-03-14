(ns seazme.sources.confluence
  (:require
   [me.raynes.fs :as fs]
   [clj-time.coerce :as tr]
   [clj-time.format :as tf]
   [clojure.java.io :refer [file]])
  (:use seazme.sources.confluence-api seazme.sources.scheduler seazme.sources.common))


(def confluence-type-name "confluence");;TODO this or rahter kind inall

(def conf-ts-formatter (tf/formatters :date-time))

(defn parse-page [kind bu instance base-url content]
  (let [p content
        space-id (-> content :space :id str) ;;TODO why str
        space-name (-> content :space :name)
        space-key (-> content :space :key)
        page-id (-> content :id)
        page-title (-> content :title)
        text (-> content :body :storage :value strip-html-tags)]
    {:id (format "%s\\%s\\%s\\%s\\%s" kind bu instance space-id page-id)
     :url (str base-url (-> content :_links :webui))
     :level0 space-key
     :level1 page-title
     :kind-name kind
     :bu-name bu
     :instance-name instance
     :parent-id ""
     :last-author (-> content :version :by :username)
     :last-ts (-> (->> content :version :when (tf/parse conf-ts-formatter) tr/to-long) (quot 1000))
     :text text
     :text-size (inc (quot (count text) 1024))}))

(defn read-page![f] (-> f slurp read-string))

;;
;; initial scan ;;TODO rename
;;
;;TODO log exceptions
(def expands "expand=space,version,ancestors,container.description,history,body.storage")
(defn- save-page-from-search [db-path api p]
  (try
    (let [page-id (-> p :id)
          page-title (-> p :title)
          _ (print "saving:" page-id page-title)
          _ (flush)
          content (api-single api (format "/rest/api/content/%s?%s" page-id expands))]
      (spit (str db-path "/" page-id) (pr-str content))
      (println " . done"))
    (catch Exception e (println "could not download page:" (:id p))
           (when-not (-> e Throwable->map :data :status (= 404)) ;;TODO double check legit 404 for 2017-04-10_11:05
             (throw e)))))

(defn- save-page-from-space [db-path p]
  (let [space-id (-> p :space :id str);;TODO why str
        space-name (-> p :space :name)
        page-id (-> p :id)
        page-title (-> p :title)
        path-name (str db-path "/" space-id)]
    (println "saving:" space-id page-id space-name page-title)
    (fs/mkdirs path-name)
    (spit (str path-name "/" page-id) (pr-str p))))

(defn save-space-from-search [api db-path s]
  (fs/mkdirs db-path)
  (try
    (let [#_ (prn (str "/rest/api/content/?spaceKey=" (:key s) "&" expands))
          ps (api-follow-link api (str "/rest/api/content/?spaceKey=" (:key s) "&" expands))
          space-id (-> s :id str)]
      (Thread/sleep 1000)
      (->> ps (map (partial save-page-from-space db-path)) doall)
      (spit (str db-path "/" space-id ".edn") (pr-str s)))
    (catch Exception e (println "could not download space:" (-> s :id str))
           (when-not (-> e Throwable->map :data :status (= 500)) ;;TODO double check legit 404 for 2017-04-10_11:05
             (throw e)))))

(defn- save-page-from-search2 [datahub-callback api p]
  (try
    (let [page-id (-> p :id)
          page-title (-> p :title)
          _ (println "posting:" page-id page-title) ;;include space name
          content (api-single api (format "/rest/api/content/%s?%s" page-id expands))]
      (datahub-callback content))
    (catch Exception e (println "could not download page:" (:id p))
           (when-not (-> e Throwable->map :data :status (= 404)) ;;TODO double check legit 404 for 2017-04-10_11:05
             (throw e)))))

(defn- save-page-from-space2 [datahub-callback p]
  (let [space-id (-> p :space :id str);;TODO why str
        space-name (-> p :space :name)
        page-id (-> p :id)
        page-title (-> p :title)
        ]
    (println "posting:" (keys p) space-id page-id space-name page-title)
    (datahub-callback p)
    ))

(defn save-space-from-search2 [api datahub-callback s]
  (try
    (let [_ (prn (str "/rest/api/content/?spaceKey=" (:key s) "&" expands))
          ps (api-follow-link api (str "/rest/api/content/?spaceKey=" (:key s) "&" expands))
          ;;space-id (-> s :id str)
          ]
      (Thread/sleep 1000)
      (->> ps (map (partial save-page-from-space2 datahub-callback)) doall)
      ;;(spit (str db-path "/" space-id ".edn") (pr-str s))
      )
    (catch Exception e (println "could not download space:" (-> s :id str))
           (when-not (-> e Throwable->map :data :status (= 500)) ;;TODO double check legit 404 for 2017-04-10_11:05
             (throw e)))))

;;TODO skip any private pages

(defn find-spaces [api] (api-follow-link api "/rest/api/space?expand=space.description.view"))

#_(defn save-spaces [api db-path cokie pred] ;; e.g. #(-> % :id (= 47906817)) skip some spaces to recreate an error
    (->> (find-spaces api) (filter pred) (map (partial db-path save-space api)) doall))

;;TODO "/rest/api -> "/rest/api and add "rest in config
;;TODO review 300 limit
;;
;; incremental scan
;;

;; Examples:
;;  https://confluence.*/rest/api/content/search?cql=(lastModified > now("-1500m") AND lastModified<now("-500m"))&limit=300
;;  https://confluence.*/rest/api/content/search?cql=(lastModified >= "2017-03-20 16:10" AND lastModified < "2017-03-20 18:10")&limit=300
(def f2 (java.text.SimpleDateFormat. "yyyy-MM-dd HH:mm"))
(defn find-updated-pages-between [api mins-from mins-to]
  (let [from (. f2 format mins-from)
        to (. f2 format mins-to)
        url (format "/rest/api/content/search?cql=(lastModified >= \"%s\" AND lastModified < \"%s\")&limit=300" from to)] ;;TODO returns 200 and empty if not logged in; 300 is to address psssible limitation in Confluence
    (api-follow-link api url)))


(defn pull-confl-incr [api db-path]
  (let [inv-f (fn[path from to]
                (let [edits (find-updated-pages-between api from to)
                      _ (when-not (empty? edits);;TODO prone for killing (now less since we depend on .last.edn), is spit atomic, is mkdir and spit in a single trx;still need rmdir (use create tmp file and  mv)
                          (spit (file path "edits.edn") (pr-str edits)));;DO NOT check if path exists, driven by `invoke-since-last-run!`
                      _ (prn "started - processing:" from to)
                      page-edits (->> edits (filter #(-> % :type (= "page"))))]
                  (->> page-edits (map #(save-page-from-search path api %)) doall)
                  (prn "completed - processing:" from to)
                  (Thread/sleep 10000)))]
    (invoke-since-last-run! db-path inv-f)));;TODO logout during the pull and check error handling

(defn pull-confl-incr2 [api datahub-callback from to]
  (let [edits (find-updated-pages-between api from to)
        page-edits (->> edits (filter #(-> % :type (= "page"))))]
    (->> page-edits (map (partial save-page-from-search2 datahub-callback api)) doall)))


;;
;; find all cached pages
;;
(defn find-pages[^java.io.File dir]
  (->>
   dir
   clojure.java.io/file
   file-seq
   (remove #(.isDirectory %))
   (filter #(->> % .getName (re-matches #"\d+")))))


;;
;; manual test code
;;
(comment
  (def co-api (mk-conf-api :get ":confluence-url" ":confluence-api-cookie" 60000))
  (def co-api (mk-conf-api-auth :get ":confluence-url" ":confluence-basic-auth" 60000))

  (find-updated-pages-between co-api 1490221581483 1490222691483)
  )


;;
;; TODO
;;
;; it is assumed that we will never exceed limit of 300, maybe not
;; timezone/GMT - it is 100% clear what TZ Confluence is actually using
