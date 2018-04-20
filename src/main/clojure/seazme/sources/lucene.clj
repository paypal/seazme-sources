;;
;; NOT USED, NOT UPDATED, EXAMPLES ONLY
;;

(ns seazme.sources.lucene
  (:require [clojure.data.json :as json]
            [clucy.core :as c])
  (:import [org.jsoup Jsoup]))


(defn strip-html-tags [s] (.text (Jsoup/parse s)))

(defn to-clucy[r]
  (let [c (-> r slurp read-string)
        author (-> c :version :by :username)
        webui (-> c :_links :webui)
        text (-> c :body :storage :value strip-html-tags)]
    {:author author :ui webui :text text}))

(def index (c/disk-index "db/confluence.lucene"))

(defn update-clucy[s]
  (->> s (map (fn[x] (println "adding:" (:ui x)) (c/add index x))) distinct))

(defn update-index[^java.io.File d]
  (let [files (->> d clojure.java.io/file file-seq (remove #(.isDirectory %)) (filter #(->> % .getName (re-matches #"\d+"))))]
    (->> files (map to-clucy) update-clucy doall)))

;;https://lucene.apache.org/core/2_9_4/queryparsersyntax.html

;;playground code
(comment
  (update-index "db/confluence")
  (-> index (c/search "IBAN" 3) pprint)
  (-> index (c/search "POC" 3) pprint)
  )
