(ns scylla.cas-only-register
    "Linearizable, single-register operations backed by lightweight transactions."
    (:require [clojure [pprint :refer :all]]
              [clojure.tools.logging :refer [info]]
              [jepsen
               [client      :as client]
               [checker     :as checker]
               [generator   :as gen]
               [independent :as independent]
               [nemesis     :as nemesis]]
              [jepsen.tests.linearizable-register :as lr]
              [jepsen.checker.timeline :as timeline]
              [knossos.model :as model]
              [qbits.alia :as alia]
              [qbits.hayt :refer :all]
              [scylla [client :as c]])
    (:import (clojure.lang ExceptionInfo)
             (com.datastax.driver.core.exceptions UnavailableException
                                                  WriteTimeoutException
                                                  ReadTimeoutException
                                                  NoHostAvailableException)))
  
  (defrecord CasRegisterClient [tbl-created? conn]
    client/Client
    (open! [this test node]
      (assoc this :conn (c/open test node)))
  
    (setup! [_ test]
      (let [session (:session conn)]
        (locking tbl-created?
          (when (compare-and-set! tbl-created? false true)
            (alia/execute session (create-keyspace :jepsen_keyspace
                                                   (if-exists false)
                                                   (with {:replication {:class :SimpleStrategy
                                                                        :replication_factor 3}})))
            (alia/execute session (use-keyspace :jepsen_keyspace))
            (alia/execute session (create-table :lwt_only
                                                (if-exists false)
                                                (column-definitions {:id    :int
                                                                     :value :int
                                                                     :primary-key [:id]})
                                                (with {:compaction {:class (:compaction-strategy test)}})))))))
  
    (invoke! [_ _ op]
      (let [s (:session conn)]
        (c/with-errors op #{:read}
          (alia/execute s (use-keyspace :jepsen_keyspace))
          (case (:f op)
            :cas (let [[k [old new]] (:value op)
                       result (alia/execute s
                                            (update :lwt_only
                                                    (set-columns {:value new})
                                                    (where [[= :id k]])
                                                    (only-if [[:value old]]))
                                            (c/write-opts test))]
                   (c/assert-applied result)
                   (assoc op :type :ok))
  
            :write (let [[k v] (:value op)
                         result (alia/execute s
                                  (update :lwt_only
                                          (set-columns {:value v})
                                          ;(only-if [[:in :value (range 5)]])
                                          (where [[= :id k]]))
                                  (c/write-opts test))]
                     (if (c/applied? result)
                       ; Great, we're done
                       (assoc op :type :ok)
  
                       ; Didn't exist, back off to insert
                       (do (c/assert-applied
                             (alia/execute s (insert :lwt_only
                                                     (values [[:id k]
                                                              [:value v]])
                                                     (if-exists false))
                                           (c/write-opts test)))
                           (assoc op :type :ok))))
  
            :read (let [[k _] (:value op)
                        v     (->> (alia/execute s
                                                 (select :lwt_only (where [[= :id k]]))
                                                 (merge {:consistency :serial}
                                                        (c/read-opts test)))
                                   first
                               :value)]
                    (assoc op :type :ok :value (independent/tuple k v)))))))
  
    (close! [_ _]
            (c/close! conn))
  
    (teardown! [_ _])
  
    client/Reusable
    (reusable? [_ _] true))
  
  
  (defn cas-register-client
    "A CAS register implemented using LWT"
    []
    (->CasRegisterClient (atom false) nil))

  (defn w   [_ _] {:type :invoke, :f :write, :value 0})
  (defn cas [_ _] (map (fn[x] {:type :invoke, :f :cas, :value [(rand-int 10) x]}) (range 0 10 1))) 

  (defn workload
    [opts]
    {:checker (independent/checker
                (checker/compose
                  {:linearizable (checker/linearizable
                                   {:model (:model opts (model/cas-register))})
                   :timeline     (timeline/html)}))
     :generator (independent/concurrent-generator
                       10
                       (range)
                       (fn [k]
                        (cond->> (->> (gen/once w)
                                      (gen/limit 3)
                                      (gen/then cas))
                        ; We randomize the limit a bit so that over time, keys
                        ; become misaligned, which prevents us from lining up
                        ; on Significant Event Boundaries.
                          (:per-key-limit opts)
                          (gen/limit (* (+ (rand 0.1) 0.9)
                                        (:per-key-limit opts 20)))
               
                          true
                          (gen/process-limit (:process-limit opts 20)))))
      :client (cas-register-client)})