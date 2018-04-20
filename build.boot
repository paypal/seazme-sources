#!/usr/bin/env boot

(set-env!
 :dependencies '[[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [byte-streams "0.2.0"]
                 [ch.qos.logback/logback-classic "1.1.1"]
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
                 [org.jsoup/jsoup "1.8.3"]]
 :source-paths   #{"src/main/clojure"})

(require '[boot.cli :as cli]
         '[clojure.core.match :refer [match]]
         '[seazme.sources.config :as config]
         '[seazme.sources.es :as es]
         '[seazme.sources.datahub :as dh])

(cli/defclifn -main
  "Executes data miner for Confluence, Twiki, mbox, etc based on options and configuration defined in config.edn. Depending on a context, only some combinations of options and configuration are valid."
  [a action VALUE str "action: reinit, scan, update"
   c context VALUE kw "kw"
   d destination VALUE kw "destination name"
   s source VALUE kw "source  name"]
  (let [a action
        c (config/config context)
        d (config/config destination)
        s (config/config source)]
    (println (match [a c d s]
                    ;;main dispatch, it is not perfect and there might be some corner cases when config is bad
                    ;;TODO make it very type aware and validate everything
                    ;;ElasticSearch
                    ["reinit" (c :guard some?)     (d :guard some?)         (s :guard nil?)]      (es/reinit! c (es/mk-es-connection d))

                    ;;ElasticSearch
                    ["scan"   {:kind "twiki"}      {:kind "elasticsearch"}  {:kind "twiki"}]      (es/twiki-scan! c (es/mk-es-connection d) s)
                    ["scan"   {:kind "confluence"} {:kind "cache"}          {:kind "confluence"}] (es/confluence-scan-2cache! c d (es/mk-conf-api s))
                    ["scan"   {:kind "confluence"} {:kind "elasticsearch"}  {:kind "cache"}]      (es/confluence-scan-2index! c (es/mk-es-connection d) s)
                    ["update" {:kind "confluence"} {:kind "cache"}          {:kind "confluence"}] (es/confluence-update-cache! c d (es/mk-conf-api s))
                    ["update" {:kind "confluence"} {:kind "elasticsearch"}  {:kind "cache"}]      (es/confluence-update-index! c (es/mk-es-connection d) s)

                    ;;DataHub
                    :else "options and/or config mismatch"))))

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
