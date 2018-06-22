(ns seazme.sources.circlehd
  (:require
   [clj-time.format :as tf]
   [clj-time.coerce :as tr])
  (:use seazme.common.common))

(def circlehd-ts-formatter (tf/formatters :date-time))

(defn parse-mediameta [kind bu instance base-url content]
  (let [media-id (-> content :media_id)
        text (str (->> content :tags (clojure.string/join "")) " "  (-> content :title) " " (-> content :description strip-html-tags))]
    {:id (format "%s\\%s\\%s\\%s" kind bu instance media-id)
     :url (str base-url "/" media-id)
     :level0 (-> content :channel)
     :level1 (-> content :title)
     :kind-name kind
     :bu-name bu
     :instance-name instance
     :parent-id ""
     :last-author (-> content :owner_id)
     :last-ts (-> (->> content :last_updated (tf/parse circlehd-ts-formatter) tr/to-long) (quot 1000))
     :text text
     :text-size (inc (quot (count text) 1024))}))
