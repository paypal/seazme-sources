(ns seazme.sources.common
  (:require [seazme.sources.twiki :as t]
            [seazme.sources.confluence :as c]
            [seazme.sources.jira :as j]
            [seazme.sources.circlehd :as chd]))

(def kind2parse
  {"twiki" t/parse-topic
   "confluence" c/parse-page
   "jira" j/parse-ticket
   "circlehd" chd/parse-mediameta
   })

;;TODO - ensure throw&exit if no conversion
