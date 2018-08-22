(ns seazme.sources.twiki
  (:require
   [clojure.data.json :as json])
  (:use seazme.common.common))

(def topic-info-regex #"%META:TOPICINFO\{((?:\w+=\"[^\"]+\"\s*)+)\}%\n")
(def topic-parent-regex #"%META:TOPICPARENT\{((?:\w+=\"[^\"]+\"\s*)+)\}%\n")
(defn- extract-topic[regex sample]
  (let [content (-> (re-seq regex sample) first second str)
        pairs (->> content (re-seq #"(?:(\w+)=\"([^\"]+)\"\s*)") (map rest) (map vec))]
    (into {} (for [[k v] pairs] [(keyword k) v]))))

(defn parse-topic[kind bu instance base-url {:keys [topic web content]}]
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
    {:id (format "%s\\%s\\%s\\%s\\%s" kind bu instance web topic);;TODO change / into \\ (due to URL spec and enginex) - inall (make it common fun) add actual instance
     :url (format "%s/%s/%s" base-url web topic) ;;TODO factor out
     :level0 web
     :level1 topic
     :kind-name kind ;;TODO rename to kind - inall
     :bu-name bu ;;TODO rename to BU - inall
     :instance-name instance ;;keep instance but change meaning - inall
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
