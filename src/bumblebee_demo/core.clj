(ns bumblebee-demo.core
  (:refer-clojure :exclude [get])
  (:require [clojure.data.generators :as gen]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [bumblebee.impl :as bumble])
  (:import java.util.UUID))

(defn new-uuid
  []
  (UUID/randomUUID))

(defn data-points
  [uuid]
  (let [last (atom (System/currentTimeMillis))]
    (repeatedly 100 (fn []
                      (let [time (swap! last - (gen/uniform 500 30000))]
                        {:type "observation"
                         :tracker (str uuid)
                         :date time
                         :value (gen/uniform 100 300)
                         :meta {:origin "sms"}})))))

(def ambient-data (data-points (new-uuid)))

(defn isolate-maybe
  [data isolate]
  (if (nil? isolate)
    data
    (bumble/isolate data isolate)))

(defn group-maybe
  [data group]
  (if (nil? group)
    data
    (bumble/group data group)))

(defn aggregate-maybe
  [data aggregate]
  (if (nil? aggregate)
    data
    (bumble/aggregate data aggregate)))

(defn unixify-dates
  [v]
  (if (= (type v) java.util.Date)
    (.getTime v)
    v))

(defn get
  [uri & {:keys [isolate group aggregate]
          :or {isolate nil
               group nil
               aggregate nil}}]
  (let [result  (if (= uri "/measurements")
                  {:response 200
                   :body (walk/postwalk unixify-dates
                                        (-> ambient-data
                                            (isolate-maybe isolate)
                                            (group-maybe group)
                                            (aggregate-maybe aggregate)))}
                  {:response 404
                   :body "Not found"})]
    (json/write-str result)))

(comment
  (get "/measurements")
  (get "/measurements" :isolate [#bumblebee/jsonpath ["$.date" :as date] #bumblebee/jsonpath ["$.value" :as value]])
  (get "/measurements" :isolate [#bumblebee/jsonpath ["$.date" :as date] #bumblebee/jsonpath ["$.value" :as value]] :group #bumblebee/timeseries {:element %date :resolution :minute})
  (get "/measurements" :isolate [#bumblebee/jsonpath ["$.date" :as date] #bumblebee/jsonpath ["$.value" :as value]] :group #bumblebee/timeseries {:element %date :resolution :minute} :aggregate #bumblebee/aggregator [:avg %value])
  (get "/measurements" :isolate [#bumblebee/jsonpath ["$.date" :as date] #bumblebee/jsonpath ["$.value" :as value]] :group #bumblebee/timeseries {:element %date :resolution :minute} :aggregate #bumblebee/aggregator [:stddev %value])
  )
