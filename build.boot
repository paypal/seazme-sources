#!/usr/bin/env boot

(set-env!
 :project 'seazme-sources
 :dependencies '[[org.clojure/clojure "1.7.0"]
                 [boot/base "2.7.1"]
                 [boot/core "2.7.1"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [byte-streams "0.2.0"]
                 ;[ch.qos.logback/logback-classic "1.1.1"]
                 [clj-http "2.0.1"]
                 [clj-time "0.11.0"]
                 [clojurewerkz/elastisch "3.0.0"]
                 [clucy "0.4.0"]
                 [digest "1.4.4"]
                 [environ "1.0.2"]
                 [javax.mail/mail  "1.4.7"]
                 [me.raynes/fs "1.4.4"]
                 [org.apache.james/apache-mime4j-core "0.7.2"]
                 [org.apache.james/apache-mime4j-dom "0.7.2"]
                 [org.jsoup/jsoup "1.8.3"]
                 [org.clojure/java.jdbc "0.7.3"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [org.clojure/tools.logging "0.3.1"]
                 [cbass "0.1.5"]
                 [com.grammarly/perseverance "0.1.2"]
                 [org.apache.hadoop/hadoop-common "2.6.0"] [com.google.guava/guava "15.0"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [org.apache.hive/hive-jdbc "1.2.1000.2.4.2.10-1"]
                 [org.apache.hive/hive-metastore "1.2.1000.2.4.2.10-1"]
                 [com.draines/postal "1.11.3"]
                 [hiccup "1.0.5"]
                 [danlentz/clj-uuid "0.1.7"]
                 [org.clojure/data.csv "0.1.3"]
                 [clj-jgit "0.8.9"]
                 [clj-time "0.11.0"]
                 ]
 :resource-paths   #{"resources" "src/main/clojure"}
 :repositories #(conj % '["hortonworks.extrepo" {:url "http://repo.hortonworks.com/content/repositories/releases"}]))

(require '[clj-jgit.porcelain :as p])

(defn -main [& args]
  (require 'seazme.sources.main)
  (apply (resolve 'seazme.sources.main/-main) args))

(def git-stamp (p/with-repo "." (format "%s.%s" (p/git-branch-current repo) (-> repo p/git-log first .getName (subs 0 8)))))

(def jar-name (format "%s.%s.%s.jar"
                      (get-env :project)
                      git-stamp
                      (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") (java.util.Date.))))
(deftask build
  "Builds an uberjar of this project that can be run with java -jar"
  []
  (comp
   (aot :namespace #{'seazme.entrypoint})
   (uber)
   #_(show :fileset true);; TODO Merge conflict: not adding META-INF/LICENSE
   (jar :file jar-name :main 'seazme.entrypoint)
   (sift :include #{(re-pattern jar-name)})
   (target)))
