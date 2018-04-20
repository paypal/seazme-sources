(ns seazme.sources.mbox
  (:require [byte-streams :as bs]
            [digest :as d]
            [clojure.java.io :as io]
            [clojure.string :as s])
  (:import (org.apache.james.mime4j.message DefaultMessageBuilder)
           (org.apache.james.mime4j.stream MimeConfig)
           (org.jsoup Jsoup)))

;;inspired by https://github.com/ieugen/mbox-iterator but 30 vs 300 lines of code
;;TODO this is a nice pattern, perhaps applicable check http://blog.malcolmsparks.com/?p=17
(defn lazy-file-lines
  "open a (probably large) file and make it a available as a lazy seq of lines"
  [filename]
  (letfn [(helper [rdr]
            (lazy-seq
             (if-let [line (.readLine rdr)]
               (cons line (helper rdr))
               (do (.close rdr) nil))))]
    (helper (clojure.java.io/reader filename))))

(def dmb (DefaultMessageBuilder.))
(def mc (MimeConfig.))
(.setMaxHeaderCount mc (* 1024 10))
(.setMaxHeaderLen mc (* 1024 64))
(.setMimeEntityConfig dmb mc)


;;this is super simple for POC only, need better parsing ... question do we keep all email threads, or top one ...
(defn from-split[s] (-> s (clojure.string/split #" From: ") first))

;;.getReader already takes care of base64 which is both surprising and not unensuring if for all cases
(defn decode-bodypart[bp] [(.getMimeType bp) (.getContentTransferEncoding bp) (.getCharset bp) (->> bp .getBody .getReader slurp Jsoup/parse .text from-split)])

(declare rec2)

(defn rec3 [b] (->> b .getBodyParts (mapcat rec2) vec))

(defn rec2[bp]
  (let [b (.getBody bp)]
    (if (.isMultipart bp)
      (rec3 b)
      (when (#{"text/html" "text/plain"} (.getMimeType bp))
        [(decode-bodypart bp)]))))

(defn rec1[m]
  (let [b (.getBody m)]
    (if (.isMultipart m)
      (rec3 b)
      [(decode-bodypart m)])))

(declare tmp2)

(defn tmp3 [b] (->> b .getBodyParts (map tmp2) (remove nil?) set))

(defn tmp2[bp]
  (let [b (.getBody bp)]
    (if (.isMultipart bp)
      [(.getMimeType bp) (tmp3 b)]
      (when (#{"text/html" "text/plain"} (.getMimeType bp))
        (decode-bodypart bp)))))

(defn tmp1[m]
  (let [b (.getBody m)]
    (if (.isMultipart m)
      [(.getMimeType m) (tmp3 b)]
      (decode-bodypart m))))

;;http://www.eyrich-net.org/mozilla/X-Mozilla-Status.html?en
(defn- abc[f s] (fn[x] (f #(.startsWith % s) x)))
(defn extract-fields [file [k l]]
  (let [x-moz-stat (Integer/parseInt (subs (->> l ((abc filter "X-Mozilla-Status:")) first) 18) 16)
        x-moz-stat2 (Integer/parseInt (subs (->> l ((abc filter "X-Mozilla-Status2:")) first) 19) 16)
        raw (clojure.string/join "\n" ((abc remove "X-Mozilla-Status") l))
        si (count raw)
        m (.parseMessage dmb (bs/to-input-stream raw))
        b (bean m)
        h (.getHeader m)
        _ (when (nil? (->> b :messageId)) (prn "XXX" b k))]
    {
     :x-moz-stat x-moz-stat
     :x-moz-stat2 x-moz-stat2
     :from (->> b :from (map #(-> % bean (select-keys [:address :name]))) first)
     :to (->> b :to (map #(-> % bean (select-keys [:address :name]))))
     :subject (->> b :subject)
     :messageId (->> b :messageId s/trim)
     :replyTo (when-let [rt (->> b :replyTo)] (s/trim rt))
     :digest (d/sha-1 raw)
     :date (-> b :date .getTime (quot 1000))
     :size (quot si 1024)
     :message m
     #_:raw #_raw}))

(defn process-mbox[file]
  (println "processing mbox" file)
  (let [a (lazy-file-lines file)
        b (partition 2 (partition-by (partial re-matches #"^From \S+.*\s\d{4}$") a))] ;;"^From \S+@\S.*\d{4}$" or "^From \S+.*\d{4}$"
    (->> b (map (partial extract-fields file)) (remove #(-> % :x-moz-stat (bit-test 3))) )))

(defn mbox?[f]
  (if (.isFile f)
    (with-open [s (io/input-stream f)]
      (= (list 70 114 111 109 32 45 32) (repeatedly 7 #(.read s))))))

;;
;; TODO not here
;;
(def instance-name "PP")
(def mbox-type-name "DL")

(defn format-mbox[e]
  {:id (-> e :messageId)
   :url (-> e :subject)
   :level0 "level0"
   :level1 "level1"
   :instance-name instance-name
   :type-name mbox-type-name
   :parent-id ""
   :last-author (-> e :from :address)
   :last-ts (-> e :date)
   :text (-> e :message rec1 first last)
   :text-size (-> e :size)})


(comment
  (def ff (file-seq (io/as-file ".../Thunderbird/Profiles/....default")))
  (def fff (filter mbox? ff)))
