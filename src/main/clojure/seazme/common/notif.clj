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

(def css "
  <style>
  table {
    font-family: arial, sans-serif;
    border-collapse: collapse;
    width: 100%;
  }

  td, th {
    border: 1px solid #dddddd;
    text-align: left;
    padding: 8px;
  }

  tr:nth-child(even) {
    background-color: #dddddd;
  }
  </style>
  ")

(defn send-email[from to subject content]
  (m/send-message  {:host (-> config/config :mta)}
                   {:from from
                    :to to
                    :subject subject
                    :body
                    (let [cid (java.util.UUID/randomUUID)
                          powered (str "<br><p>Powered by <img src=cid:" cid " height=20 width=40></p>")]
                      [:related
                       {:type "text/html"
                        :content (str "<html><head>"css"</head><body>" content powered " </body></html>")
                        }
                       {:type :inline
                        :content-id cid
                        :content (java.io.File. "1407951412s.png")
                        :content-type "image/png"
                        }]
                      )
                    }))
