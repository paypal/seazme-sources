(ns seazme.scripts.analytics-jira-fields
  (:require [seazme.sources.jira-api :as jira-api]
            [clojure.data.json :as json] [clojure.edn :as edn] [clojure.java.io :as io] [clojure.data.csv :as csv]
            [clj-time.format :as tf] [clj-time.coerce :as tr] [clj-time.core :as tc]
            [clojure.core.match :refer [match]])
  (:use seazme.scripts.common seazme.common.common))

;;
;; parameters
;;
(def work-dir (str (System/getProperty "user.home") "/seazme-tmpdir"))
(def conf-file (str work-dir "/config.edn"))
(def cfmap-file (str work-dir "/cfmap.csv"))

;;
;; STEPS
;;

;;1 extract data from SeazMe cache (assuming it has been produced) or DataHub itself (WIP)
(comment
  ;;1 update cache
  )

;;2 extract project/customfield data
(comment
  (def path "...")
  (def f1 (->> path clojure.java.io/file file-seq (filter #(.isFile %)) sort))
  (count f1)
  ;;real
  ;; on source machine
  (a1 f1 "agg1")
  (u2v "agg1" "agg2cf" anacf)
  (u2v "agg1" "agg2p" anap)
  (v2csv "agg2cf" csvcf)
  (v2csv "agg2p" csvp)

  ;;test
  (comment
    (with-open [in (-> "agg1.edn.gz" io/input-stream java.util.zip.GZIPInputStream. io/reader java.io.PushbackReader.)]
    (let[edn-seq (repeatedly (partial edn/read {:eof nil} in))]
      (def aaa (->> edn-seq (take-while (partial not= nil)) (remove empty?) (take 100) doall))))

    (->> aaa (map anap) pprint)
    (->> aaa (map anacf) (reduce (partial merge-with +)) pprint)
    (->> aaa (map anap) (reduce (partial merge-with +)) pprint))  
  )

;;3 exctract field use
(comment
  ;;mkdir o
  (def path "...")
  (def f1 (->> path clojure.java.io/file file-seq (filter #(.isFile %)) #_(filter (comp neg? #(compare "2019-01-01" %) #(.getName %))) sort))
  (->> f1 (map (partial scan-file proct)) dorun)
  (->> @cf-files vals (map #(.close %)) dorun)(reset! cf-files {})

  ;;rm attachment.edns.gz comment.edns.gz parent.edns.gz timetracking.edns.gz votes.edns.gz watches.edns.gz worklog.edns.gz issuelinks.edns.gz subtasks.edns.gz
  (def path2 "o")
  (def f2 (->> path2 clojure.java.io/file file-seq (filter #(.isFile %))))
  (def stats (->> f2 (pmap f2p)))
  (s-to-csv "cfuse.csv" stats)
  )


;;4 copy files to a laptop
(comment
  ;; scp/cp and ungzip "*.gz" to "work-dir"
  )

;;5 prepare mapping file, on laptop
(comment
  (produce-cf-file conf-file cfmap-file)
  )

;;
;; common code
;;
(defn s-to-csv [f d] (with-open [out-file (io/writer f)] (csv/write-csv out-file d)))
(defn subs* [s b e] (subs s b (min e (count s))))

;;
;; field use
;;
(defn newfile1 [cfk] (-> (str "o/" (name cfk) ".edns.gz") clojure.java.io/output-stream java.util.zip.GZIPOutputStream. clojure.java.io/writer))
(def cf-files (atom {}))
(defn newfile2 [[cfk cfv]]
  (swap! cf-files new-if-not-exists cfk #(newfile1 cfk))
  (let [f (@cf-files cfk)]
    (.write f (prn-str cfv))
    (.newLine f)))

(defn proct[t]
  (prn "processing:" (:key t))
  (->> t :fields (filter (comp not nil? second)) (pmap newfile2) doall))

(defn dispatch[fn e]
  (let [S java.lang.String I java.lang.Long D java.lang.Double]
    (match [e (class e)]
           [v S] (subs* v 0 42)
           [v I] v
           [v D] v
           [{:progress _, :total _} _] e
           [{:value v} _] v
           [{:name v} _] v
           [[ & r ] _] (clojure.string/join "," (sort (map (partial dispatch fn) r)))
           :else (throw (Exception. (str fn ":" (pr-str e)))))))

(defn f2p[fs];;TODO convert to scan file!
  (with-open [in (-> fs clojure.java.io/input-stream java.util.zip.GZIPInputStream. clojure.java.io/reader java.io.PushbackReader.)]
    (let[edn-seq (repeatedly (partial edn/read {:eof nil} in))
         fn (.getName fs)
         pieces (->> edn-seq
                     (take-while (partial not= nil))
                     #_(take 10)
                     (map (partial dispatch fn)))
         frq (frequencies pieces)
         va (vals frq)
         ma (apply max va)
         su (reduce + va)]
      [fn ma su (double (/ ma su))])))


;;
;; mapping file
;;

(defn produce-cf-file[conf-file cfmap-file]
  (let [config (-> conf-file slurp read-string)
        credentials (-> config :credentials)
        url (-> config :url)
        pja (jira-api/mk-pja-api :get jira-api/pja-rea-400 url credentials false)
        cfs (-> (pja "api/2/field") :body (json/read-str :key-fn keyword))]
    (->> cfs (map #(map % [:id :name])) (s-to-csv cfmap-file))))


;;
;; extracting data from JIRA
;;
(def jira-ts-formatter (tf/formatters :date-time))
(defn since2007[d] (tc/in-weeks (tc/interval (tc/date-time 2007) (tf/parse jira-ts-formatter d))))
(defn ese3[e] [(-> e :fields :updated since2007) (-> e :fields :project :key) (->> e :fields (filter val) keys)])
(defn ese2[e] [(-> e :fields :updated since2007) (->> e :fields (filter val) keys)])

(defn write-to-stream-1[w s] (.write w (pr-str s)) (.newLine w))
(defn write-to-stream[w s] (locking w #_(prn (count s)) (.write w (pr-str s)) (.newLine w)))
;;(defn- write-to-stream[w s] (prn (count s))(.write w (pr-str s)) (.newLine w))


;;TODO  copy docs from paper
(defn anacf [s] (frequencies (mapcat (fn[[k1 k2 vv]] (map #(vector k1 k2 %) vv)) s)))
(defn anap [s] (frequencies (map (fn[[k1 k2 vv]] (vector k1 k2)) s)))
(defn csvcf [out [[k1 k2 v] c]] (.write out (str k1","k2","(name v)","c)) (.newLine out))
(defn csvp [out [[k1 k2] c]] (.write out (str k1","k2","c)) (.newLine out))
(defn u2v[u v f]
  (with-open [in (-> (str u ".edn.gz") io/input-stream java.util.zip.GZIPInputStream. io/reader java.io.PushbackReader.)
              out (-> (str v ".edn.gz") io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
    (let[edn-seq (repeatedly (partial edn/read {:eof nil} in))]
      (->> edn-seq
           (take-while (partial not= nil))
           (remove empty?)
           (map f)
           (map (partial write-to-stream-1 out))
           dorun))))
(defn v2csv[v f]
  (with-open [in (-> (str v ".edn.gz") io/input-stream java.util.zip.GZIPInputStream. io/reader java.io.PushbackReader.)
              out (-> (str v ".csv.gz") io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
    (let[edn-seq (repeatedly (partial edn/read {:eof nil} in))
         pieces (->> edn-seq
                     (take-while (partial not= nil))
                     (reduce (partial merge-with +)))]
      (->> pieces (map (partial f out)) dorun))))

(defn a1[f1 v]
  (with-open [w (-> (str v ".edn.gz") clojure.java.io/output-stream java.util.zip.GZIPOutputStream. clojure.java.io/writer)]
   (->> f1 (pmap (partial scan-file ese3)) (map (partial write-to-stream w)) dorun)))
