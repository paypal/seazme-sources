#!/usr/bin/env boot

(set-env!
 :dependencies '[[org.clojure/clojure "1.7.0"]
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
                 [javax.mail/mail  "1.4.5"]
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
                 [com.sun.mail/javax.mail "1.5.5"]
                 [com.draines/postal "1.11.3"]
                 [hiccup "1.0.5"]
                 ]
 :resource-paths #{"resources"}
 :source-paths   #{"src/main/clojure"})

(require '[boot.cli :as cli]
         '[clojure.core.match :refer [match]]
         '[seazme.common.config :as config]
         '[seazme.common.es :as es]
         '[seazme.sources.direct2es :as d2e]
         '[seazme.sources.datahub :as dh]
         '[seazme.sources.hbase2es :as h2e]
         '[seazme.sources.snapshot :as ss]
         '[clojure.tools.logging :as log])

(defn- run-main[action context destination source parameters continue]
  (let [a action
        c (config/config context)
        d (config/config destination)
        s (config/config source)
        p parameters
        o continue]
    (println (match [a c d s p]
                    ;;main dispatch, it is not perfect and there might be some corner cases when config is bad
                    ;;TODO make it very type aware and validate everything
                    ;;ElasticSearch
                    ["reinit" (c :guard some?)     (d :guard some?)         (s :guard nil?)       (p :guard nil?)]  (d2e/reinit! c (es/mk-connection d))
                    ["reinitdatasources" (c :guard nil?)  (d :guard some?)  (s :guard nil?)       (p :guard nil?)]  (d2e/reinit-datasources! (es/mk-connection d))

                    ;;ElasticSearch (pre HBASE version, still works)
                    ["scan"   {:kind "twiki"}      {:kind "elasticsearch"}  {:kind "twiki"}       (p :guard nil?)]  (d2e/twiki-scan! c (es/mk-connection d) s)
                    ["scan"   {:kind "confluence"} {:kind "cache"}          {:kind "confluence"}  (p :guard nil?)]  (d2e/confluence-scan-2cache! c d (d2e/mk-conf-api s))
                    ["scan"   {:kind "confluence"} {:kind "elasticsearch"}  {:kind "cache"}       (p :guard nil?)]  (d2e/confluence-scan-2index! c (es/mk-connection d) s)
                    ["update" {:kind "confluence"} {:kind "cache"}          {:kind "confluence"}  (p :guard nil?)]  (d2e/confluence-update-cache! c d (d2e/mk-conf-api s))
                    ["update" {:kind "confluence"} {:kind "elasticsearch"}  {:kind "cache"}       (p :guard nil?)]  (d2e/confluence-update-index! c (es/mk-connection d) s)

                    ;;DataHub
                    ["scan"   {:kind "twiki"}      {:kind "datahub"}        {:kind "twiki"}       (p :guard nil?)]  (dh/twiki-scan! c d s)
                    ["scan"   {:kind "confluence"} {:kind "datahub"}        {:kind "confluence"}  (p :guard nil?)]  (dh/confluence-scan! c d (d2e/mk-conf-api s))
                    ["update" {:kind "confluence"} {:kind "datahub"}        {:kind "confluence"}  (p :guard nil?)]  (dh/confluence-update! c d (d2e/mk-conf-api s) o)
                    ["scan"   {:kind "jira"}       {:kind "datahub"}        {:kind "jira"}        (p :guard nil?)]  (dh/jira-scan! c d s)
                    ["patch"  {:kind "jira"}       {:kind "datahub"}        {:kind "jira"}        (p :guard some?)] (dh/jira-patch! c d s p)
                    ["update" {:kind "jira"}       {:kind "datahub"}        {:kind "jira"}        (p :guard nil?)]  (dh/jira-update! c d s o)
                    ["scan"   {:kind "jira"}       (d :guard nil?)          {:kind "jira"}        (p :guard nil?)]  (dh/jira-scan-to-cache! c s)
                    ["patch"  {:kind "jira"}       (d :guard nil?)          {:kind "jira"}        (p :guard some?)] (dh/jira-patch-to-cache! c d s p)
                    ["scan"   {:kind "snow"}       {:kind "datahub"}        {:kind "snow"}        (p :guard nil?)]  (dh/snow-scan! c d s)

                    ;;HBASE (reusing context, need args)
                    ["update" {:kind "hbase"}      {:kind "elasticsearch"}  _                     (p :guard nil?)]  (h2e/process-sessions! c (es/mk-connection d))
                    ["update" {:kind "hbase"}      (d :guard nil?)          _                     (p :guard nil?)]  (ss/process-sessions! c)
                    :else "options and/or config mismatch"))))

(cli/defclifn -main
  "Executes data miner for Confluence, Twiki, mbox, etc based on options and configuration defined in config.edn. Depending on a context, only some combinations of options and configuration are valid."
  [a action VALUE str "action: reinit, scan, update"
   c context VALUE kw "kw"
   d destination VALUE kw "destination name"
   s source VALUE kw "source name"
   p parameters VALUE str "context specific parameters"
   o continue bool "indicates if update shall continue until DataHub returns 202 (no more data to pull)"]
  (try
    (run-main action context destination source parameters continue)
    (catch Exception ex (do
                          (prn ex "-main failed to execute")
                          (log/error ex "-main failed to execute")))))

;;TODO fix docs
(deftask build
  "Builds an uberjar of this project that can be run with java -jar"
  []
  (comp
   (aot :namespace #{'main.entrypoint})
   (uber)
   ;; (jar :file "project.jar" :main 'main.entrypoint)
   (jar :file "project.jar")
   (sift :include #{#"project.jar"})
   (target)))
