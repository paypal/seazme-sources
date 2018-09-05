(ns seazme.common.notif
  (:require [hiccup.core :as h]
            [postal.core :as m]
            [clojure.string :as s]
            [seazme.common.config :as config]))


;;example
(defn mhtml-format-github-demo[org repos]
  (h/html
   [:h2 (str "Organization: " org)]
   [:h3 " repositories:"]
   [:ul
    (for [repo repos]
      [:li "GitHub: " [:a {:href (str "https://github.com/" repo)} repo]]
      )]))

(defn send-email[subject content]
  (m/send-message  {:host (-> config/config :notif :host)}
                   {:from (-> config/config :notif :from)
                    :to (-> config/config :notif :to)
                    :subject subject
                    :body
                    (let [cid (java.util.UUID/randomUUID)
                          powered (str "<br><p>Powered by <img src=cid:" cid " height=20 width=40></p>")]
                      [:related
                       {:type "text/html"
                        :content (str "<html><head></head><body>" content powered " </body></html>")
                        }
                       {:type :inline
                        :content-id cid
                        :content (java.io.File. "1407951412s.png")
                        :content-type "image/png"
                        }]
                      )
                    }))
