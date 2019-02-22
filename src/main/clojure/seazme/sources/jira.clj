(ns seazme.sources.jira
  (:require
   [clj-time.format :as tf]
   [clj-time.coerce :as tr]
   [clj-time.core :as tc]
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
(defn- write-to-stream[w s] (.write w (pr-str s)) (.newLine w))
(def oldest-issue "2007/01/01 00:00")
(def oldest-issue-DT (tf/parse ff2 oldest-issue))
(def until-issue-DT (tc/minus (tc/now) (tc/days 1))) ;;leave 24h margin, assumed that full scan spans to now-48h so there is always an overlap
(defn date-since-first[r] (tc/plus oldest-issue-DT (tc/hours (* 3 r))))
(defn find-periods[]
  (->>
   (range)
   (map date-since-first)
   (take-while #(tc/before? % until-issue-DT)) ;;cannot do dynamic since there is no guarantees how early sequence is realized
   (map #(tr/to-long %))
   (partition 2 1)))

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


(defn period-search-full[pja-search-api period cb]
  (let [[from to] (->> period (map tr/to-date-time) (map (partial tf/unparse ff2)))]
    (jira-api/pja-search-full pja-search-api (format "updated >= '%s' and updated < '%s' ORDER BY updated ASC" from to) cb)))

(defn period-search-all-full[pja-search-api period cb]
  (let [[from to] (->> period (map tr/to-date-time) (map (partial tf/unparse ff2)))]
    (jira-api/pja-search-all-full pja-search-api from to cb)))

(def period-search period-search-all-full)

(defn- upload-period-full-stream[{:keys [kind instance index]} cached? skip-cache pja-search-api callback-fn period]
  (Thread/sleep (+ 10 (rand-int 100)))
  (let [period2 (->> period (map tr/to-date-time) (map (partial tf/unparse ff3)))
        msg (str (tc/now)" downloading JIRAs for:"(pr-str period2)" "cached?" "kind" "instance" "index)
        base-path (format "db/%s-cache/%s/%s" kind instance index)
        _ (fs/mkdirs base-path)
        file-path (apply format "%s/%s-%s.edn.gz" base-path period2)
        future-file-path (str file-path ".future")]
    (if (and cached? (fs/exists? file-path))
      (do
        (when-not skip-cache
          (println (str msg"... only form cache"))
          (with-open [in (-> file-path io/input-stream java.util.zip.GZIPInputStream. io/reader java.io.PushbackReader.)]
            (let[counter (atom 0)
                 edn-seq (repeatedly (wrap-with-counter counter (partial edn/read {:eof nil} in)))]
              (dorun (map callback-fn (take-while (partial not= nil) edn-seq)))
              (println (str msg"...pulled:"@counter))))))
      (if cached?
        (do
          (println (str msg"... and cache"))
          (with-open [w (-> future-file-path io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
            (let [cb (combine-fun-calls (partial write-to-stream w) callback-fn)
                  cnt (count (period-search pja-search-api period cb))]
              (println (str msg"...pulled:"cnt))))
          (fs/rename (fs/file future-file-path) (fs/file file-path)))
        (do
          (println (str msg"... no cache"))
          (let [cnt (count (period-search pja-search-api period callback-fn))]
            (println (str msg"...pulled:"cnt))))))))
(def upload-period upload-period-full-stream)

(defn upload-by-jql[{:keys [kind instance index]} pja-search-api callback-fn jql]
  (with-open [w (-> "dump.edn.gz" io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
    (let [_ (println (str (tc/now)) "downloading JIRAs for:" jql)
          cb (combine-fun-calls (partial write-to-stream w) callback-fn)
          ret1 (jira-api/pja-search-full pja-search-api (format "%s ORDER BY updated ASC" jql) cb)]
      (print "pulled" (count ret1))
      (println "... done"))))

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

  (defn upload-period-2apicall[{:keys [kind instance index]} cached? pja-search-api pja-issue-full f period]
    (let [period2 (->> period (map tr/to-date-time) (map (partial tf/unparse ff3)))
          _ (print "downloading JIRAs for:" period2 cached? kind instance index)
          _ (flush)
          base-path (format "db/%s-cache/%s/%s" kind instance index)
          _ (fs/mkdirs base-path)
          file-path (apply format "%s/%s-%s.edn" base-path period2)
          jiras (if (and cached? (fs/exists? (str file-path ".gz")))
                  (do
                    (print  "... form cache")
                    (uncompress! file-path))
                  (let [jiras1 (jira-api/pja-search pja-search-api (apply format "updated >= '%s' and updated < '%s'" period3) identity);;this needs to be fixed, if used in future at all
                        process-jira (fn [i] (-> i :id pja-issue-full :body (json/read-str :key-fn keyword)))
                        jiras2 (map process-jira jiras1)]
                    (when cached?
                      (print  "... writing")
                      (compress! file-path jiras2))
                    jiras))
          _ (println "... pulled" (count jiras2))]
      (->> jiras
           (map f) doall)
      ))

  (defn upload-period-1apicall-inmem[{:keys [kind instance index]} cached? skip-cache pja-search-api f period]
    (try
      (let [period2 (->> period (map tr/to-date-time) (map (partial tf/unparse ff3)))
            _ (print "downloading JIRAs for:" period2 cached? kind instance index)
            _ (flush)
            base-path (format "db/%s-cache/%s/%s" kind instance index)
            _ (fs/mkdirs base-path)
            file-path (apply format "%s/%s-%s.edn" base-path period2)
            jiras (if (and cached? (fs/exists? (str file-path ".gz")))
                    (do
                      (print  "... form cache")
                      (when-not skip-cache
                        (uncompress! file-path)))
                    (let [jiras (period-search pja-search-api period identity)]
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


  (defn upload-period-1apicall-partially-stream[{:keys [kind instance index]} cached? skip-cache pja-search-api f period]
  (try
    (let [period2 (->> period (map tr/to-date-time) (map (partial tf/unparse ff3)))
          _ (print "downloading JIRAs for:" period2 cached? kind instance index)
          _ (flush)
          base-path (format "db/%s-cache/%s/%s" kind instance index)
          _ (fs/mkdirs base-path)
          file-path (apply format "%s/%s-%s.edn.gz" base-path period2)]
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
          (let [jiras (period-search pja-search-api period identity)]
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
