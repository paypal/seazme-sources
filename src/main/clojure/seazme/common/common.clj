(ns seazme.common.common
  (:require [clj-time.format :as tf] [clj-time.core :as tr] [clj-time.coerce :as te])
  (:import [org.jsoup Jsoup]))

(defn strip-html-tags [s] (.text (Jsoup/parse s)))

(defn file-exists [f])
(defn to-edn [f d] (spit f (pr-str d)))
(defn from-edn [f] (read-string (slurp f)))

(defn pmapr[r f coll] (->> coll (partition-all r) (mapcat #(->> % (pmap f) doall))))

(defn jts-now[] (te/to-long (tr/now)))
