(ns seazme.sources.scheduler
  (:require
   [me.raynes.fs :as fs]
   [clojure.java.io :refer [file]]))

(defn sincenmin [n] (- (.getTime (java.util.Date.)) (* 1000 60 n)))
(def one-min (* 1000 60))
(def five-min (* 1000 60 5))
(def six-min (* 1000 60 6))
(defn find-time-segments[last-run delta]
  (let [round-time (* (quot last-run one-min) one-min) now (.getTime (java.util.Date.))]
    (range round-time now delta)))

(def f1 (java.text.SimpleDateFormat. "yyyy-MM-dd_HH:mm"))
(defn f1parse[s] (try (. f1 parse s) (catch Exception _ nil)))

(def last-file "/.last.edn")
(defn get-last-file[dir] (-> dir (str "/" last-file) slurp read-string))

(defn write-last-file[dir content] (-> dir (str "/" last-file) (spit (pr-str content))))

(defn get-incr-dirs[dir]
  (->>
   dir
   fs/list-dir
   ;;TODO filter dirs
   sort
   (map #(hash-map
          :file %
          :path+file (str dir "/" %)))
   (remove #(-> % :file f1parse nil?))
   (remove #(-> % :file (= (get-last-file dir))))
   seq))

;;that funtion might re-invoke last timestamp until the current time is out of its window
(defn invoke-since-last-run![dir f]
  (let [last-dir-ts (->> dir get-last-file f1parse .getTime) ;;TODO crashes when `last-file` does not exist
        tss (find-time-segments last-dir-ts five-min)
        invoke-f #(let [new-file (. f1 format %)
                        new-dir (str dir "/" new-file)]
                    (Thread/sleep 1000)
                    (write-last-file dir new-file)
                    (fs/mkdir new-dir);;TODO create dir only if there is an output
                    (f (fs/file new-dir) % (+ % six-min)))]
    (dorun (map invoke-f tss))))

(defn invoke-and-backup![dir proc-subdir f]
  (when-let [dirs (get-incr-dirs dir)]
    (let [processed-dir (str dir "/" proc-subdir)
          _ (fs/mkdir processed-dir)
          invoke-f #(do
                      (Thread/sleep 100)
                      (f %)
                      (.renameTo (file (:path+file %)) (file processed-dir (:file %))))]
      (fs/mkdir processed-dir)
      (dorun (map invoke-f dirs)))))

;;
;; manual test code
;;
(comment
  (. f1 format (sincenmin 10))
  (->> (map #(java.util.Date. %) (find-time-segments (sincenmin 100) five-min)) pprint)
  )
