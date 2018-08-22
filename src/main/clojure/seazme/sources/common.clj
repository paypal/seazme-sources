(ns seazme.sources.common
  (:require [seazme.common.hbase :as hb]
            [seazme.sources.twiki :as t]
            [seazme.sources.confluence :as c]
            [seazme.sources.jira :as j]))

(def kind2parse
  {"twiki" t/parse-topic
   "confluence" c/parse-page
   "jira" j/parse-ticket
   })
