(ns seazme.sources.snow
  (:require [clj-time.format :as tf]
            [clj-time.core :as tc]
            [clj-http.client :as c]
            [clojure.data.json :as json]))

(def ff (tf/formatter "YYYY-MM-dd"))
(def ff2 (tf/formatter "YYYY-MM-dd HH:mm:ss"))

(def oldest-issue "2016-11-01")
(def oldest-issue-DT (tf/parse ff oldest-issue))
(def until-issue-DT (tc/minus (tc/now) (tc/days 1))) ;;leave 24h margin, assumed that full scan spans to now-48h so there is always an overlap
(defn date-since-first[r] (tc/plus oldest-issue-DT (tc/hours (* 3 r))))
(defn find-periods[]
  (->>
   (range)
   (map date-since-first)
   (take-while #(tc/before? % until-issue-DT)) ;;cannot do dynamic since there is no guarantees how early sequence is realized
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
