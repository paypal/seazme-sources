(ns seazme.sources.common
  (:import [org.jsoup Jsoup]))

(defn strip-html-tags [s] (.text (Jsoup/parse s)))


(defn file-exists [f])
