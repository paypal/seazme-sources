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
                 [clojurewerkz/elastisch "3.0.0-beta2"]
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
         '[seazme.sources.config :as config])

(use 'seazme.sources.es)

(cli/defclifn -main
  "Executes data miner for Confluence, Twiki, mbox, etc based on options and configuration defined in config.edn. Depending on a context, only some combinations of options and configuration are valid."
  [d data-source VALUE str "data source: twiki, conf, mbox"
   a action VALUE str "action: reinit-index, upload-index, update-index, upload-cache, update-cache"
   s search-profile VALUE kw "search profile name"
   c conf-profile VALUE kw "confluence profile name"
   e es-profile VALUE kw "elastic search profile name"]
  (let [d data-source
        a action
        s (config/config search-profile)
        c (config/config conf-profile)
        e (config/config es-profile)]
    (println (match [d a s c e]
                    [(d :guard nil?) "reinit-index" (s :guard some?) (c :guard nil?)  (e :guard some?)] (reinit-index! (mk-es-connection e) s)
                    ["conf"          "upload-cache" (s :guard some?) (c :guard some?) (e :guard nil?)]  (confluence-upload-cache! (mk-conf-api c) s)
                    ["conf"          "update-cache" (s :guard some?) (c :guard some?) (e :guard nil?)]  (confluence-update-cache! (mk-conf-api c) s)
                    ["conf"          "upload-index" (s :guard some?) (c :guard nil?)  (e :guard some?)] (confluence-upload-index! (mk-es-connection e) s)
                    ["conf"          "update-index" (s :guard some?) (c :guard nil?)  (e :guard some?)] (confluence-update-index! (mk-es-connection e) s)
                    ["twiki"         "upload-index" (s :guard some?) (c :guard nil?)  (e :guard some?)] (twiki-upload-index! (mk-es-connection e) s)
                    ["mbox"          "upload-index" (s :guard some?) (c :guard nil?)  (e :guard some?)] (prn "WIP")
                    :else "options and/or config mismatch"))))
