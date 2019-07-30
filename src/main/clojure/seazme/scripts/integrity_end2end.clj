(ns seazme.scripts.integrity-end2end
  (:require [seazme.sources.jira-api :as jira-api]
            [seazme.common.hbase :as hb]
            [clojure.java.jdbc :as j]
            [seazme.common.config :as config]
            [seazme.common.notif :as n]
            [perseverance.core :as p]
            [clojure.set :as s]
            [hiccup.core :as h]))

;;hive shell
;;> INSERT OVERWRITE LOCAL DIRECTORY '<full-path-to-destination-dir>' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select <fields,separated> FROM <hivetable> where projectid in (<'PROJECT',sparated>)
;;gunzip *;cat * > o.csv
(defn hive-get-projects-fingerprint[table project]
  (prn "running HIVE for:" project)
  (hb/get-conn)
  (->> (j/query (-> config/config :hive) (format "select jiraid,updated,status from %s where projectid in ('%s')" table project))
       (map #(mapv % [:jiraid :status :updated]))))

(defn jira-get-projects-fingerprint[pja-search-api project]
  (prn "running JIRA for:" project)
  (->> (jira-api/pja-search pja-search-api (str "project=" project) {:fields ["updated" "status"]} identity)
       (map (fn [{key :key {updated :updated {status :name} :status} :fields}] [key status updated]))
       distinct
       doall))
(defn hive-get-projects-fingerprint* [& args] (p/retry {} (p/retriable {:catch [Exception]} (apply hive-get-projects-fingerprint args))))

#_(defn mp25[x] (some #(.startsWith x (str % "-")) prjs))


(defn- reshape [l] (into {} (for [[a & r] l] [a r])))
(defn diff-projects [l r]
  (let [ld (reshape l)
        rd (reshape r)
        all-keys (->> (concat (keys ld) (keys rd)) distinct)
        is-same (fn [l r] (if-not (= l r) [l r]))
        check-diff (fn [k] (let [l (ld k) r (rd k)] [k (is-same l r)]))]
    (->> all-keys (map check-diff) (remove (comp nil? second)) (into {}))))

(defn mhtml-diffs[diffs]
  (when-not (empty? diffs)
    [:table
     [:tr [:th "JIRA Id"] [:th "HIVE"] [:th "JIRA"]]
     (for [[jk [l r]] (sort-by first diffs)] [:tr [:td jk] [:td (clojure.string/join ", " l)] [:td (clojure.string/join ", " r)]])
    ]))

(defn mhtml-format[dp]
  (h/html
   [:h2 "JIRA projects:"]
   [:ul
    (for [[p d] dp]
      [:li [:h3 p] (mhtml-diffs d)])]))

(defn run[{:keys [table]} d s]
  (let [pja-search-api (jira-api/mk-pja-search-api (:url s) (:credentials s) (:debug s))
        diff (for [p (d :projects)]
               [p (diff-projects (hive-get-projects-fingerprint* table p) (jira-get-projects-fingerprint pja-search-api p))])]
    (n/send-email (d :from) (d :to) (d :subject) (mhtml-format diff))))
