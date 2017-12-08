(ns seazme.sources.snow
  (:require [clj-time.format :as tf]
            [clj-time.core :as tc]
            [clj-http.client :as c]
            [clojure.data.json :as json]))

(def ff (tf/formatter "YYYY-MM-dd"))
(def ff2 (tf/formatter "YYYY-MM-dd HH:mm:ss"))

(def oldest-issue "2016-11-01")
(def oldest-issue-DT (tf/parse ff oldest-issue))
(def newest-issue-DT (tc/plus (tc/now) (tc/days 1))) ;;TODO addumed that it is a one time run batch,  tc/now is timesensitive :-)
(defn date-since-first[r] (tc/plus oldest-issue-DT (tc/days r)))
(defn find-periods[]
  (->>
   (range)
   (map date-since-first)
   (take-while #(tc/before? % newest-issue-DT))
   (map #(tf/unparse ff2 %))
   (partition 2 1)))

(defn mk-snow-api[config]
  (let [;;url (str (:host dhs-config) "/v1/datahub/")
        url "https:// service-now API /api/now"
        options {;;:insecure? true
                 :throw-exceptions true
                 :basic-auth config
                 :as :json}]
    (fn [api-path]
      (:body (c/get (str url api-path) options)))))

(def range-query-old "/table/task?sysparm_query=sys_updated_onBETWEENjavascript:gs.dateGenerate('%s')@javascript:gs.dateGenerate('%s')")
(def range-query "/table/task?sysparm_query=sys_updated_on>=javascript:gs.dateGenerate('%s')^sys_updated_on<javascript:gs.dateGenerate('%s')")
(defn upload-period[api f period]
  (println "downloading SNOWs for:" period)
  (let [snows (api (apply format range-query period))]
    (->> snows :result (map f) doall)))
