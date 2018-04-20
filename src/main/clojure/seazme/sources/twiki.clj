(ns seazme.sources.twiki
  (:require
   [clojure.data.json :as json])
  (:use seazme.sources.common))

(def instance-name "PP")
(def twiki-type-name "twiki")

(def topic-info-regex #"%META:TOPICINFO\{((?:\w+=\"[^\"]+\"\s*)+)\}%\n")
(def topic-parent-regex #"%META:TOPICPARENT\{((?:\w+=\"[^\"]+\"\s*)+)\}%\n")
(defn- extract-topic[regex sample]
  (let [content (-> (re-seq regex sample) first second str)
        pairs (->> content (re-seq #"(?:(\w+)=\"([^\"]+)\"\s*)") (map rest) (map vec))]
    (into {} (for [[k v] pairs] [(keyword k) v]))))

(defn parse-topic[{:keys [topic web content]}]
  (let [r clojure.string/replace
        text (->
              content
              (r #"%.*%\n" "")
              ;;add %TABLE{}% or %ADDTOZONE{}%
              (r #"--+\++" "")
              (r #"\[\[.*\]\]" "")
              (r #"\s+\* " "")
              ;;before this point replaces assume mutliple lines
              (r "\n" " ")
              (r #"<verbatim>.*?</verbatim>" "")
              (r #"\s+\d+" "")
              (r "*" "")
              (r "|" "")
              (r " +" " ")
              (r "%TOC%" "")
              (r #"<em>" "");;TODO does not work
              (r #"</em>" "")
              strip-html-tags
              )
        page-attrs (merge (extract-topic topic-info-regex content)
                          (extract-topic topic-parent-regex content))]
    {:id (format "%s/%s/%s/%s" instance-name twiki-type-name web topic)
     :url (format "https://*/wiki/%s/%s" web topic)
     :level0 web
     :level1 topic
     :instance-name instance-name
     :type-name twiki-type-name
     :parent-id ""
     :last-author (page-attrs :author)
     :last-ts (page-attrs :date)
     :text text
     :text-size (inc (quot (count text) 1024))}))

;;
;; scan
;;
(defn read-topic![^java.io.File f]
  (try
    {:topic (clojure.string/replace (.getName f) #".txt" "")
     :web (.getName (.getParentFile f))
     :content (slurp f)}
    (catch Exception e (println "could not read:" f) nil)))

;;
;; find all topics
;;
(defn find-topics[^java.io.File d]
  (->>
   d
   clojure.java.io/file
   file-seq
   (remove #(.isDirectory %))
   (filter #(->> % .getName (re-matches #"^[^*&%\s]+\.txt")))))
