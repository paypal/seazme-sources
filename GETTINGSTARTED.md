
# Getting Started

Ideally you've already setup the [Seazme Data Hub].  If not go check out the [getting started pages].
Also install [Boot]!

## First register your source

To do this go to the swagger api for the Seazme Data Hub and fill in the 

## Configure sources 

You'll need to register your source on the data hub intake form.  You will replace the `:app-id`
in the following `config.edn` file.  The `app-id` will look like a uuid like `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`

### A Basic Local JIRA Config
```
;; config.edn
{
 ;;Context
 :context-hbase         {
                         :kind "hbase"
                         :prefix "test"
                         }

 :context-demo-jira       {
                         :kind "jira"
                         :index "demo-jira"
                         :instance "DEMO"
                           :app-id "<TODO - register your app with http://localhost:3000/index.html#!/intake/post_v1_datahub_applications_intake>"
                         }
 ;;-Cache
 :confluence-demo-cache {
                       :kind "cache"
                       :path "db/conf"
                       }
 ;;-JIRA
 :jira-prod {
             :kind "jira"
             :credentials ["*" "*"]
             :cache true
             :url "https://*/jira/rest"
             }
 ;;-DataHub
 :datahub-demo {
                :kind "datahub"
                :host "http://localhost:3000"
                :basic-auth ["open" "seazme"]
                :parallel-factor 1
                }
 }
```

### Populate Jira Data

These commands will run the jira scanner and update the data in the data hub.  You'll want to set them up to run on regular interval. 

```
./data-miner -a scan -c context-demo-jira -d datahub-demo -s jira-prod
./data-miner -a update -c context-demo-jira -d datahub-demo -s jira-prod
```

[boot]: https://github.com/boot-clj/boot
[Seazme Data Hub]: https://github.com/paypal/seazme-hub/
[getting started pages]:  https://github.com/paypal/seazme-hub/GETTINGSTARTED.md
