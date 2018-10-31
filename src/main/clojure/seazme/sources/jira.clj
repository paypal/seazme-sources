(ns seazme.sources.jira
  (:require
   [clj-time.format :as tf]
   [clj-time.coerce :as tr]
   [clj-time.core :as tc]
   [clj-time.coerce :as co]
   [clojure.data.json :as json]
   [seazme.sources.jira-api :as jira-api]
   [me.raynes.fs :as fs]
   [clojure.java.io :as io]
   [clojure.edn :as edn]
   [clojure.java.io :refer [file]]
   ))

(def jira-ts-formatter (tf/formatters :date-time))

(def ff2 (tf/formatter "YYYY/MM/dd HH:mm"));;TODO fix names!
(def ff3 (tf/formatter "YYYY-MM-dd_HH:mm"))

(defn- wrap-with-counter[counter f] (fn[& args] (swap! counter inc) (apply f args)))
(defn- combine-fun-calls[& fns] (fn[& args] (doall (map #(apply % args) fns))))
(defn- write-to-stream[w s] (.write w (pr-str s)))
(def oldest-issue "2013/01/01 12:00")
(def oldest-issue-DT (tf/parse ff2 oldest-issue))
(def newest-issue-DT (tc/plus (tc/now) (tc/days 1))) ;;TODO addumed that it is a one time run batch,  tc/now is timesensitive :-)
(defn date-since-first[r] (tc/plus oldest-issue-DT (tc/days r)))
(defn find-periods[]
  (->>
   (range)
   (map date-since-first)
   (take-while #(tc/before? % newest-issue-DT))
   (map #(co/to-long %))
   (partition 2 1)
   #_butlast;;TODO this is necessary for full scan to have correct time range but .... also last has to be manually removed when running scan again
   ))

(defn parse-ticket [kind bu instance base-url content]
  (let [p content
        ticket-id (-> content :id)
        ticket-key (-> content :key)
        text (-> content :fields :description)]
    {:id (format "%s\\%s\\%s\\%s" kind bu instance ticket-id)
     :url (str base-url "/browse/" ticket-key)
     :level0 (-> content :fields :project :key)
     :level1 (-> content :fields :summary)
     :kind-name kind
     :bu-name bu
     :instance-name instance
     :parent-id ""
     :last-author (-> content :fields :creator :name)
     :last-ts (-> (->> content :fields :updated (tf/parse jira-ts-formatter) tr/to-long) (quot 1000))
     :text text
     :text-size (inc (quot (count text) 1024))}))

(defn- upload-period-full-stream[{:keys [kind instance index]} cached? skip-cache pja-search f period]
  (let [period2 (map co/to-date-time period)
        period3 (map (partial tf/unparse ff2) period2)
        _ (print (str (tc/now)) "downloading JIRAs for:" period3 cached? kind instance index);;TODO unify ts in logs
        _ (flush)
        base-path (format "db/%s-cache/%s/%s" kind instance index)
        _ (fs/mkdirs base-path)
        file-path (apply format "%s/%s-%s.edn.gz" base-path (map (partial tf/unparse ff3) period2))
        future-file-path (str file-path ".future")]
    (if (and cached? (fs/exists? file-path))
      (do
        (when-not skip-cache
          (print  "... form cache")
          (with-open [in (-> file-path io/input-stream java.util.zip.GZIPInputStream. io/reader java.io.PushbackReader.)]
            (let [counter (atom 0)
                  edn-seq (repeatedly (wrap-with-counter counter (partial edn/read {:eof nil} in)))]
              (dorun (map f (take-while (partial not= nil) edn-seq)))
              (print "... pulled" @counter)))))
      (if cached?
        (do
          (with-open [w (-> future-file-path io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
            (let [cb (combine-fun-calls (partial write-to-stream w) f)
                  jira-ids (jira-api/pja-search-full2 pja-search (apply format "updated >= '%s' and updated < '%s'" period3) cb)]
              (print "... pulled" (count jira-ids))))
          (fs/rename (fs/file future-file-path) (fs/file file-path)))
        (do
          (let [jira-ids (jira-api/pja-search-full2 pja-search (apply format "updated >= '%s' and updated < '%s'" period3) f)]
            (print "... pulled" (count jira-ids))))))
    (println "... done"))
  )
(def upload-period upload-period-full-stream)

(comment ;;older version, might not be compatible any more
  (defn- compress! [fn s]
    (let [final-path (str fn ".gz")
          tmp-path (str final-path ".tmp")]
      (with-open [w (-> tmp-path
                        clojure.java.io/output-stream
                        java.util.zip.GZIPOutputStream.
                        clojure.java.io/writer)]
        (.write w (pr-str s)))
      (fs/rename (fs/file tmp-path) (fs/file final-path))))

  (defn- uncompress! [fn]
    (-> fn (str ".gz") io/input-stream java.util.zip.GZIPInputStream. io/reader java.io.PushbackReader. edn/read))

  (defn convert [sd dd fn]
    (let [sfn (str sd "/" fn)
          dfn (str dd "/" fn ".gz")
          tmp-dfn (str dfn "-tmp")]
      (if (fs/exists? dfn)
        (println "existed:" dfn)
        (do
          (print "converting:" dfn)
          (with-open [w (-> tmp-dfn io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
            (dorun (map (partial write-to-stream w) (uncompress! sfn))))
          (fs/rename (fs/file tmp-dfn) (fs/file dfn))
          (println "converted:" dfn)))))

  (defn upload-period-2apicall[{:keys [kind instance index]} cached? pja-search pja-issue-full f period]
    (let [period2 (map co/to-date-time period)
          period3 (map (partial tf/unparse ff2) period2)
          _ (print "downloading JIRAs for:" period3 cached? kind instance index)
          _ (flush)
          base-path (format "db/%s-cache/%s/%s" kind instance index)
          _ (fs/mkdirs base-path)
          file-path (apply format "%s/%s-%s.edn" base-path (map (partial tf/unparse ff3) period2))
          jiras (if (and cached? (fs/exists? (str file-path ".gz")))
                  (do
                    (print  "... form cache")
                    (uncompress! file-path))
                  (let [jira-ids (jira-api/pja-search pja-search (apply format "updated >= '%s' and updated < '%s'" period3))
                        process-jira (fn [i] (-> i :id pja-issue-full :body (json/read-str :key-fn keyword)))
                        jiras (map process-jira jira-ids)]
                    (when cached?
                      (print  "... writing")
                      (compress! file-path jiras))
                    jiras))
          _ (println "... pulled" (count jiras))]
      (->> jiras
           (map f) doall)
      ))

  (defn upload-period-1apicall-inmem[{:keys [kind instance index]} cached? skip-cache pja-search f period]
    (try
      (let [period2 (map co/to-date-time period)
            period3 (map (partial tf/unparse ff2) period2)
            _ (print "downloading JIRAs for:" period3 cached? kind instance index)
            _ (flush)
            base-path (format "db/%s-cache/%s/%s" kind instance index)
            _ (fs/mkdirs base-path)
            file-path (apply format "%s/%s-%s.edn" base-path (map (partial tf/unparse ff3) period2))
            jiras (if (and cached? (fs/exists? (str file-path ".gz")))
                    (do
                      (print  "... form cache")
                      (when-not skip-cache
                        (uncompress! file-path)))
                    (let [jiras (jira-api/pja-search-full pja-search (apply format "updated >= '%s' and updated < '%s'" period3))]
                      (when cached?
                        (print  "... writing")
                        (compress! file-path jiras))
                      jiras))
            _ (print "... pulled" (count jiras))]
        (->> jiras
             (map f) doall)
        (println "... done")
        )
      (catch Exception e (do (prn "ERROR" period e) []))
      )
    )


  (defn upload-period-1apicall-partially-stream[{:keys [kind instance index]} cached? skip-cache pja-search f period]
  (try
    (let [period2 (map co/to-date-time period)
          period3 (map (partial tf/unparse ff2) period2)
          _ (print "downloading JIRAs for:" period3 cached? kind instance index)
          _ (flush)
          base-path (format "db/%s-cache/%s/%s" kind instance index)
          _ (fs/mkdirs base-path)
          file-path (apply format "%s/%s-%s.edn.gz" base-path (map (partial tf/unparse ff3) period2))]
      (if (and cached? (fs/exists? file-path))
        (do
          (when-not skip-cache
            (print  "... form cache")
            (with-open [in (-> file-path io/input-stream java.util.zip.GZIPInputStream. io/reader java.io.PushbackReader.)]
              (let [counter (atom 0)
                    edn-seq (repeatedly (wrap-with-counter counter (partial edn/read {:eof nil} in)))]
                (dorun (map f (take-while (partial not= nil) edn-seq)))
                (print "... pulled" @counter)))))
        (do
          (let [jiras (jira-api/pja-search-full pja-search (apply format "updated >= '%s' and updated < '%s'" period3))]
            (when cached?
              (print  "... writing")
              (with-open [w (-> file-path io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
                (dorun (map (partial write-to-stream w) jiras))))
            (dorun (map f jiras))
            (print "... pulled" (count jiras)))))
      (println "... done"))
    (catch Exception e (do (prn "ERROR" period e) []))
    ))


  (defn- wrap-with-counter[counter f] (fn[& args] (swap! counter inc) (apply f args)))
  (defn map-gzip-edn-steam[file-path fn]
    (with-open [in (-> file-path clojure.java.io/input-stream java.util.zip.GZIPInputStream. clojure.java.io/reader java.io.PushbackReader.)]
      (let [counter (atom 0)
            edn-seq (repeatedly (wrap-with-counter counter (partial clojure.edn/read {:eof nil} in)))
            r (doall (map fn (take-while (partial not= nil) edn-seq)))]
        (print "... pulled" @counter) r)))
  )

;;TODO
;;1- in order to make sure tickets from past are not updated in meantime, the pull needs to be repeated for the period of time when the full scan worked
;;2- make defaults for from search from from REST GET
;;3- make 2 versions of upload cached and working
;;4- TODO reload after JIRA/Confluence upgrade
