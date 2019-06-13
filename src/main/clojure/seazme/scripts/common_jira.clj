(ns jira-common
  (:require
   [clj-time.format :as tf]
   [clj-time.core :as tc]))


(defn- flatten-history[his] (map #(assoc % :created (:created his)) (:items his)))
(defn changelog-items[j] (->> j :changelog :histories (mapcat flatten-history)))

;;
;; misc
;;
(defn s-to-edn [f d] (spit f (with-out-str (pr d))))
(defn s-from-edn [f] (read-string (slurp f)))
(defn cache-dir [& xs] (apply str "cache/" xs))
(defn select-values [map ks] (reduce #(conj %1 (map %2)) [] ks))
(def jira-ts-formatter (tf/formatters :date-time))
(defn jira-mk-ts[s] (tf/parse jira-ts-formatter s))
(defn jira-ts-before[ts this] (tc/before? (jira-mk-ts ts) this))
(defn jira-ts-after[ts this] (tc/after? (jira-mk-ts ts) this))
(defn jira-ts-apart[this that]
  (let [ts-this (jira-mk-ts this)
        ts-that (jira-mk-ts that)]
    (if (tc/before? ts-this ts-this)
      (- (tc/in-minutes (tc/interval ts-that ts-this)))
      (+ (tc/in-minutes (tc/interval ts-this ts-that))))))
(defn add-is-parent[s]
  (let [valid-tickets-ids (->> s (map :parent) (filter identity) (set))]
    (map #(assoc % :is-parent (-> % :key valid-tickets-ids not not)) s)))

;;
;; helpers
;;
(defn to-int[b] (if b 1 0))
(defn map-odd [f coll]
  (map-indexed #(if (even? %1) (f %2) %2) coll))
(def jira-timedate-format (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
(defn jira-timedate [s] (.getTime (. jira-timedate-format parse s)))
(defn jira-timedate-now[] (.getTime (java.util.Date.)))
(defn jira-ttr[fields]
  (if fields
    (let [created  (get fields :created)
          resolved (get fields :resolutiondate)
          resolved2 (if resolved (jira-timedate resolved) (jira-timedate-now))]
      (/ (- resolved2 (jira-timedate created)) (* 1000. 60 60 24)))))
;;use http://stackoverflow.com/questions/32511405/how-would-time-ago-function-implementation-look-like-in-clojure

(defn date-to-month[date] (if date (subs date 0 7)))

(defn get-or-throw [coll key]
  (if (contains? coll key)
    (get coll key)
    (throw (IllegalArgumentException. (str key)))))
(defn timedate-now[]  (.getTime (java.util.Date.)))
(defn timedate-age[ts] (quot (- (timedate-now) ts) (* 1000 60 60 24 7)))
(def not-empty? (complement empty?))
(def jira-ts-cutoff (jira-mk-ts "2015-06-01T00:00:00.000-0600"))
(defn- within-30-days?[this that] (< (jira-ts-apart this that) (* 60 24 30)))
(defn changelog-filter[filter-fields changelog-items]
  (->> changelog-items
       (filter #(contains? filter-fields (:field %)))
       (mapv #(select-keys % [:created :field :fromString :toString :from :to]))))

;;on Dec 10, 2016 JIRA priority reclassification took place. Following mapping applies:
;;Blocker (all teams 24/7) 2 -> P0 1
;;Blocker (key teams 24/7) 3 -> P1 2
;;Critical 20 -> P2 10
;;Major,Normal 40,90 -> P3 40
;;Minor,Trivial 90,90 -> P4 70
;; or "Blocker/Critical" -> "P0-2"
;;    "Major/Normal/Minor/Trivial"/"P3-4"

(defn- priority-weight[p] (get-or-throw {"Blocker" 5 "Critical" 4 "Major" 3 "Normal" 2 "Minor" 1 "Trivial" 0
                                         "P0" 5 "P1" 5 "P2" 4 "P3" 3 "P4" 1} p))
(defn- priority-group-weight[p] (get-or-throw {"Blocker" 1 "Critical" 1 "Major" 0 "Normal" 0 "Minor" 0 "Trivial" 0
                                               "P0" 1 "P1" 1 "P2" 1 "P3" 0 "P4" 0} p))
(defn high-priority?[p] (= 1 (priority-group-weight p)))

(defn escalated? [initial-priority final-priority]
  "If the initial-priority of a ticket was in Major, Normal, Minor, or Trivial and was promoted to a final_priority of Critical or Blocker-- we consider it escalated and return true. Otherwise false"
  (< (priority-group-weight initial-priority) (priority-group-weight final-priority)))

(defn deescalated? [initial-priority final-priority]
  "If the initial-priority of a ticket was in Critical or Blocker and was changed to a final-priority of anything else-- we consider it deescalated and return true. Otherwise false"
  (> (priority-group-weight initial-priority) (priority-group-weight final-priority)))

(defn priority-group-cmp
  "Compares priority groups."
  [x y] (- (priority-group-weight x) (priority-group-weight y)))

(defn priority-cmp
  "Compares prorities."
  [x y] (- (priority-group-weight x) (priority-group-weight y)))


(defn changelog-escalation[f prio-changelog-items]
    (if (not-empty prio-changelog-items)
      (f (:fromString (first prio-changelog-items)) (:toString (last prio-changelog-items)))
      false))
