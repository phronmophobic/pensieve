(ns com.phronemophobic.pensieve.xtdb
  (:require [clojure.java.io :as io]
            [com.phronemophobic.pensieve.impl.collect :as collect]
            [xtdb.api :as xt]
            [com.phronemophobic.pensieve.impl.tokenizer
             :as-alias tokenizer]
            [com.phronemophobic.pensieve.impl.hprof
             :as hprof]))


(defn- start-xtdb! []
  (letfn [(kv-store [dir]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir (io/file dir)
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store "data/dev/tx-log")
      :xtdb/document-store (kv-store "data/dev/doc-store")
      :xtdb/index-store (kv-store "data/dev/index-store")})))

(defmulti ->transaction :type)

(defmethod ->transaction :HPROF_UTF8 [m]
  {:xt/id {:id (:id m)}
   :hprof/type (:type m)
   :hprof/str (:str m)})


(defmethod ->transaction :HPROF_LOAD_CLASS [m]
  {:xt/id {:object-id (:object-id m)}
   :hprof/type (:type m)
   :hprof/class-name {:id (:class-name-id m)}})

(defmethod ->transaction :HPROF_GC_INSTANCE_DUMP [m]
  {:xt/id {:object-id (:object-id m)}
   :hprof/type (:type m)
   :hprof/class-object-id {:object-id (:class-object-id m)}
   :hprof/instance-size (:instance-size m)})

(defn map-transaction
  ([rf]
   (let [m* (volatile! nil)]
     (completing
      (fn [result input]
        (let [m @m*]
          (if m
            (let [m (tokenizer/hydrate m input)]
              (if (map? m)
                (let [txn (->transaction m)]
                  (vreset! m* nil)
                  (if txn
                    (rf result txn)
                    result))
                (do
                  (vreset! m* m)
                  result)))
            (if (and (vector? input)
                     (= (first input)
                        ::tokenizer/start)
                     (get-method ->transaction (second input)))
              (do
                (vreset! m* (-> (tokenizer/hydrate)
                                (tokenizer/hydrate input)))
                result)
              result))))))))

(defn load-hprof [fname xtdb-node]
  (hprof/tokenize-hprof
   (comp map-transaction
         (map (fn [entity]
                [::xt/put entity]))
         (partition-all 5000)
         #_(map (fn [x]
                  (prn "batch!")
                  x)))
   (completing
    (fn [xtdb-node txns]
      (xt/submit-tx xtdb-node txns)
      xtdb-node)
    (fn [xtdb-node]
      (xt/sync xtdb-node)
      xtdb-node))
   xtdb-node
   fname))

(comment

  (def xtdb-node (start-xtdb!))

  (time
     (load-hprof
      "../dewey/examples/cosmos/cosmos.hprof"
      ;; "../schematic/schematic2.hprof"
      xtdb-node
      ))
  
  (xt/q (xt/db xtdb-node)
        '{:find [?id ?classname]
          :limit 10
          :where [[?id :hprof/type :HPROF_LOAD_CLASS]
                  [?id :hprof/class-name ?class]
                  [?class :hprof/str ?classname]]})

  (def sizes
    (time
     (xt/q (xt/db xtdb-node)
           '{:find [?class-name (sum ?instance-size)]
             :where [[?id :hprof/type :HPROF_GC_INSTANCE_DUMP]
                     [?id :hprof/instance-size ?instance-size]
                     [?id :hprof/class-object-id ?class]
                     [?class :hprof/class-name ?str]
                     [?str :hprof/str ?class-name]]})))



  (defn- stop-xtdb! []
    (.close xtdb-node))

  ,)


