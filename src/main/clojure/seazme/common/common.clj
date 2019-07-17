(ns seazme.common.common
  (:require [clj-time.format :as tf] [clj-time.core :as tr] [clj-time.coerce :as te])
  (:import [org.jsoup Jsoup]))

(defn strip-html-tags [s] (.text (Jsoup/parse s)))

(defn file-exists [f])
(defn to-edn [f d] (spit f (pr-str d)))
(defn from-edn [f] (read-string (slurp f)))

(defn pmapr-old[r f coll] (->> coll (partition-all r) (mapcat #(->> % (pmap f) doall)))) ;;TODO make it lazy no doall

(defn pmapr[n f s]
  (let [pool (java.util.concurrent.Executors/newFixedThreadPool n)
        tasks (for [i s] #(f i))
        futures (.invokeAll pool tasks)]
    (.shutdown pool)
    (for [ftr futures] (.get ftr))))

(defn jts-now[] (te/to-long (tr/now)))

(let [lock (Object.)]
  (defn sync-println [& args]
    (locking lock (apply println args))))
