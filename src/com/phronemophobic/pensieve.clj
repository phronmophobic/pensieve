(ns com.phronemophobic.pensieve
  (:refer-clojure :exclude [read] )
  (:require [com.phronemophobic.pensieve.impl.hprof
             :as hprof]
            [clojure.java.io :as io])
  (:import (com.sun.management HotSpotDiagnosticMXBean)
           (java.lang.management ManagementFactory)))

(defn heap-dump
  "Dumps the heap to the outputFile file in the same format as the hprof heap dump.

  `output-path` - the system-dependent filename
  `live?` - if true dump only live objects i.e. objects that are reachable from others"
  [output-path live?]
  (let [b ^HotSpotDiagnosticMXBean
        (ManagementFactory/newPlatformMXBeanProxy
         (ManagementFactory/getPlatformMBeanServer)
         "com.sun.management:type=HotSpotDiagnostic"
         HotSpotDiagnosticMXBean)]
    (.dumpHeap b output-path live?)))


(defn hprof-info [fname]
  (hprof/hprof-info fname))

(defn tokenize
  ([fname]
   (hprof/tokenize-hprof fname))
  ([xform rf init fname]
   (hprof/tokenize-hprof xform rf init fname)))

(comment
  (def info (time
             (hprof-info "../schematic/schematic2.hprof")))

  (def tokens
    (time
     (tokenize
      (hprof/find-key :str)
      conj
      []
      "../dewey/examples/cosmos/cosmos.hprof")))

  (def tokens
    (time
     (tokenize
      (comp (map (constantly 1)))
      +
      0
      "../dewey/examples/cosmos/cosmos.hprof")))

  ,)
