(ns seazme.sources.common
  (:import [org.jsoup Jsoup]))

(defn strip-html-tags [s] (.text (Jsoup/parse s)))

(defn file-exists [f])
(defn to-edn [f d] (spit f (pr-str d)))
(defn from-edn [f] (read-string (slurp f)))

(defn pmapr[r f coll] (->> coll (partition-all r) (mapcat #(->> % (pmap f) doall))))
