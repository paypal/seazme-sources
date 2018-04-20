(ns seazme.sources.log
  (:require [clojure.pprint :as pprint])
  (:import [ch.qos.logback.classic Level Logger]
           [java.io StringWriter]
           [org.slf4j LoggerFactory MDC]))

;;(defn make-logger[name] ^ch.qos.logback.classic.Logger (LoggerFactory/getLogger name))
(defn make-logger[name] (LoggerFactory/getLogger name))

(defn set-log-level!
  "Pass keyword :error :info :debug"
  [logger level]
  (case level
    :debug (.setLevel logger Level/DEBUG)
    :info (.setLevel logger Level/INFO)
    :error (.setLevel logger Level/ERROR)))

(defmacro with-logging-context [context & body]
  "Use this to add a map to any logging wrapped in the macro. Macro can be nested.
  (with-logging-context {:key \"value\"} (info \"yay\"))
  "
  `(let [wrapped-context# ~context
         ctx# (MDC/getCopyOfContextMap)]
     (try
       (if (map? wrapped-context#)
         (doall (map (fn [[k# v#]] (MDC/put (name k#) (str v#))) wrapped-context#)))
       ~@body
       (finally
         (if ctx#
           (MDC/setContextMap ctx#)
           (MDC/clear))))))

(defmacro debug [logger & msg]
  `(.debug ~logger (print-str ~@msg)))

(defmacro info [logger & msg]
  `(.info ~logger (print-str ~@msg)))

(defmacro error [logger throwable & msg]
  `(if (instance? Throwable ~throwable)
    (.error ~logger (print-str ~@msg) ~throwable)
    (.error ~logger (print-str ~throwable ~@msg))))

(defmacro spy
  [expr]
  `(let [a# ~expr
         w# (StringWriter.)]
     (pprint/pprint '~expr w#)
     (.append w# " => ")
     (pprint/pprint a# w#)
     (error (.toString w#))
     a#))

(defn demo[logger-name]
  (let [lgr (make-logger logger-name)]
    (println "---")
    (debug lgr "Test debug log")
    (println "---")
    (info lgr "Test info log")
    (println "---")
    (error lgr "Test error log")
    (println "---")
    ;;(log/spy "Spy Test")
    (println "---")
    (try (throw (Exception. "JUST A TEST. DON'T PANIC."))
         (catch Exception e (error lgr e "Test exception logging")))
    (println "^^^ This exception was expected.")
    )
  )
