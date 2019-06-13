(ns seazme.scripts.integrity
  (:require [seazme.sources.jira-api :as jira-api]
            [clojure.data.json :as json] [clojure.edn :as edn] [clojure.java.io :as io] [clojure.data.csv :as csv])
  (:use seazme.scripts.common seazme.common.common))


(comment
  "compare webhook with API"
  (defn yf[f] (-> (str "..." f ".edn") slurp read-string))
  (defn cc[f] (let [y (yf f)
                    x (jira-api/pja-search-full pja-search-api (str "key=" (-> y :issue :key)) identity)]
                (prn (-> y :issue :key))
                (->> (clojure.data/diff (-> x first :fields) (-> y :issue :fields)) (take 2) pprint)))
  (cc "<jiraid>")
  )


(comment
  "find cache dups"
  (defn dups [seq]
    (map (fn [[id freq]] id)
         (filter (fn [[id freq]] (> freq 1))
                 (frequencies seq))))

  (def cache-dir "...")
  (def files (->> cache-dir clojure.java.io/file file-seq (filter #(.isFile %)) sort)) ;;TODO sort by file name
  (defn dupsinfile[f] (->> f (scan-file :key) dups))
  (->> files (pmap #(vector % (dupsinfile %))) (remove #(-> % second empty?)) (map println) dorun)
  )


(comment
  "compare caches"

;; removing dups, not implemented
;;  boot.user=> (def ll [[:a 1] [:a 2] [:b 1] [:c 1] [:a 1]])
;;  #'boot.user/ll
;;  boot.user=> (reduce (partial apply assoc) {} ll)
;; {:a 1, :b 1, :c 1}

  (def ese #(vector (-> % :key) (-> % :fields :updated) (-> % :fields :status :name)))

  (defn gztoset[s]
    (doall (map #([(.getName %) (set (scan-file ese %))]) s)))
  (defn cmpgz[e1 e2]
    (prn (.getName e1) (.getName e2))
    (pprint (clojure.set/difference (set (scan-file ese e1)) (set (scan-file ese e2)))))

  (def f1 (->> "..." clojure.java.io/file file-seq (filter #(.isFile %)) sort))
  (def f2 (->> "..." clojure.java.io/file file-seq (filter #(.isFile %)) sort))
  (count f1)
  (count f2)
  (def r2 (->> f2 (mapcat #(scan-file ese %)) set))
  (def r1 (->> f1 (mapcat #(scan-file ese %)) set))
  (count r1)
  (count r2)
  (pprint (clojure.set/difference r2 r1))
  (count (clojure.set/difference r2 r1))

  (map cmpgz f2 f1)
 )


(comment
  "find tickets posted in many sessions"
  (defn fmap [f m] (into (empty m) (for [[k v] m] [k (f v)])))
  (def sessions (find-sessions nil))
  (def recent-sessions (->> sessions (filter (comp (partial = "jira") :kind :app)) (take-last 20) (map (comp :id :meta :self))))
  (defn keys-per-session[sid] (->> dist-bytes (pmap (partial get-data-entries-seq sid)) (mapcat identity) (map #(->> % second :self :payload :key))))

  ;;check :created for a break away timestamp
  (->> sessions (filter (comp (partial = "jira") :kind :app)) (take-last 20) (map :self) (map-indexed vector) pprint)

  (def x (map keys-per-session recent-sessions))
  (def y (map-indexed #(map (fn[k] [k %1]) %2) x))
  (def z (mapcat identity y))

  ;; pick sessions with index lower than break away
  (->> z (group-by first) (fmap #(map second %)) sort pprint)
  (pprint (map-indexed vector recent-sessions))
  )


(comment
  "verify JIRA API, there should never be new tickets when looking up for certain updated time frame in future"
  (defn fmap [f m] (into (empty m) (for [[k v] m] [k (f v)])))
  (def sessions (find-sessions nil))
  (def recent-sessions (->> sessions (filter (comp (partial = "jira") :kind :app)) (take-last 2)))
  (defn essential-per-session[session]
    (let[sid (-> session :self :meta :id)
         {:keys [from to]} (-> session :self :range)
         essef #(vector (->> % second :self :payload :key) (->> % second :self :payload :fields :updated))]
      (assert (not (nil? sid)))
      (vector sid from to (->> dist-bytes (pmap (partial get-data-entries-seq sid)) (mapcat identity) (map essef)))))

  (def x (map essential-per-session recent-sessions))
  (def y (map-indexed #(map (fn[k] [k %1]) %2) x))
  (def z (mapcat identity y))
  )


(comment
  "verify JIRA API, there should never be new tickets when looking up for certain updated time frame in future"

  (def sessions (find-sessions nil))
  (def jira-sessions (->> sessions (filter (comp (partial = "jira") :kind :app))))
  (def small-jira-sessions (->> jira-sessions (filter (comp (partial > 200) :count :self second))))
  (def recent-jira-sessions (->> jira-sessions (take-last 5)))
  (defn jiratuple[e] [(-> e :key) (-> e :fields :updated)])
  (defn jiras-per-sid[sid] (->> dist-bytes (pmap (partial get-data-entries-seq sid)) (mapcat identity) (map (comp jiratuple :payload :self second))))
  (defn jiras-per-sid-filtered[filt sid] (->> dist-bytes (pmap (partial get-data-entries-seq sid)) (mapcat identity) (map (comp :payload :self second)) (filter #(contains? filt (% :key))) set))
  (defn essential-per-session[session]
    (let[sid (-> session :self :meta :id)
         cnt (-> session :self :count)
         {:keys [from to]} (-> session :self :range)]
      (vector sid [from to] cnt (jiras-per-sid sid))))

  (defn test-pull[pja-search-api e]
    (let [[sid rnge cnt details] e
          in-datahub (set details)
          in-jira (set (jira/period-search pja-search-api rnge jiratuple))
          issub  (clojure.set/subset? in-jira in-datahub)]
      (prn "result" sid rnge issub (count in-jira) (count in-datahub) (clojure.set/difference in-jira in-datahub))
      [sid rnge in-jira in-datahub]
      ))

  (defn test-diff[e]
    (let [[sid rnge in-jira in-datahub] e]
      (prn "result" sid rnge (clojure.set/subset? in-jira in-datahub) (count in-jira) (count in-datahub) (clojure.set/difference in-jira in-datahub))))


  (def z1 (map (comp essential-per-session second) recent-jira-sessions))
  (def pja-search-api (jira-api/mk-pja-search-api (-> config/config :jira-pp-prod :url) (-> config/config :jira-pp-prod :credentials) false))
  (def z2 (->> z1 (map (partial test-pull pja-search-api)) doall))
  (->> z2 (map test-diff) dorun)


  ;;new scripts 3 attempts after refactoring
  (def s1x (jiras-per-sid "<sid1>"))
  (def s2a (jiras-per-sid "<sid2>"))
  (def s2b (jiras-per-sid "<sid3>"))
  ;;old scripts
  (def s3x (jiras-per-sid "<sid4>"))
  (def sample (set (map first (take 20 (clojure.set/intersection (set s1x) (set s2a) (set s2b) (set s3x))))))

  (def d1x (jiras-per-sid-filtered sample "<sid1>"))
  (def d2a (jiras-per-sid-filtered sample "<sid2>"))
  (def d2b (jiras-per-sid-filtered sample "<sid3>"))
  (def d3x (jiras-per-sid-filtered sample "<sid4>"))
  (= d1x d2a d2b d3x)
  )
