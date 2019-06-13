(ns seazme.scripts.common
  (:require [clojure.data.json :as json] [clojure.edn :as edn] [clojure.java.io :as io] [clojure.data.csv :as csv]))


(defn new-if-not-exists [m k fv] (if (m k) m (assoc m k (fv))))
(defn assoc-if-exists [m k v] (if (m k) (assoc m k v) m))
(comment
  "test"
  (def store (atom {:a 1}))
  (swap! store assoc-if-exists :a 4)
  (swap! store assoc-if-exists :b 4)
  (swap! store new-if-not-exists :a (fn [] 10))
  (swap! store new-if-not-exists :b (fn [] 10))
  (swap! store new-if-not-exists :b (fn [] 11)))

;;
;; common
;;
#_(defn scan-file-flat [w cb file-path]
    (println file-path)
    (with-open [in (-> file-path clojure.java.io/input-stream java.util.zip.GZIPInputStream. clojure.java.io/reader java.io.PushbackReader.)]
      (let[edn-seq (repeatedly (partial clojure.edn/read {:eof nil} in))]
        (->> edn-seq (take-while (partial not= nil)) (map cb) (map (partial write-to-stream w)) dorun))))
#_(use 'seazme.common.common)
#_(with-open [w (-> "u.edn.gz" io/output-stream java.util.zip.GZIPOutputStream. io/writer)]
    (->> f1 (pmapr 32 (partial scan-file w ese2)) dorun))

(defn scan-file [cb file-path]
  (with-open [in (-> file-path io/input-stream java.util.zip.GZIPInputStream. io/reader java.io.PushbackReader.)]
    (let[edn-seq (repeatedly (partial edn/read {:eof nil} in))]
      (doall (map cb (take-while (partial not= nil) edn-seq))))))

