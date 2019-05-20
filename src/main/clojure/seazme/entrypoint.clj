(ns seazme.entrypoint
  (:gen-class))

(defn -main [& args]
  (require 'seazme.sources.main)
  (apply (resolve 'seazme.sources.main/-main) args))
