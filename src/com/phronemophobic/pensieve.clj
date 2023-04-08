(ns com.phronemophobic.pensieve
  (:refer-clojure :exclude [read] )
  (:require [com.phronemophobic.pensieve.impl.hprof
             :as hprof]
            [clojure.java.io :as io])
  (:import (com.sun.management HotSpotDiagnosticMXBean)
           (java.lang.management ManagementFactory)))

(defn heap-dump [output-path live?]
  (let [b ^HotSpotDiagnosticMXBean
        (ManagementFactory/newPlatformMXBeanProxy
         (ManagementFactory/getPlatformMBeanServer)
         "com.sun.management:type=HotSpotDiagnostic"
         HotSpotDiagnosticMXBean)]
    (.dumpHeap b output-path live?)))


(defn hprof-info [fname]
  (hprof/hprof-info fname))

(defn tokenize [fname]
  (hprof/tokenize-hprof fname))
