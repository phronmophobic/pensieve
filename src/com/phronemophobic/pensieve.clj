(ns com.phronemophobic.pensieve
  (:refer-clojure :exclude [read] )
  (:require [com.phronemophobic.pensieve.collect :as collect]
            [clojure.math :as math]
            [clojure.java.io :as io])
  (:import (com.sun.management GarbageCollectionNotificationInfo GcInfo HotSpotDiagnosticMXBean)
           (java.lang.management ManagementFactory)
           java.io.ByteArrayInputStream
           java.math.BigInteger
           java.io.PushbackInputStream
           java.io.InputStream
           java.util.Date))




;; https://hg.openjdk.org/jdk/jdk/file/9a73a4e4011f/src/hotspot/share/services/heapDumper.cpp
(def record-tags
  { ;; // top-level records
   :HPROF_UTF8                     0x01
   :HPROF_LOAD_CLASS               0x02
   :HPROF_UNLOAD_CLASS             0x03
   :HPROF_FRAME                    0x04
   :HPROF_TRACE                    0x05
   :HPROF_ALLOC_SITES              0x06
   :HPROF_HEAP_SUMMARY             0x07
   :HPROF_START_THREAD             0x0A
   :HPROF_END_THREAD               0x0B
   :HPROF_HEAP_DUMP                0x0C
   :HPROF_CPU_SAMPLES              0x0D
   :HPROF_CONTROL_SETTINGS         0x0E
   ;; // 1.0.2 record types
   :HPROF_HEAP_DUMP_SEGMENT        0x1C
   :HPROF_HEAP_DUMP_END            0x2C
   ;; // field types


   })

;; /*
;; * HPROF binary format - description copied from:
;; *   src/share/demo/jvmti/hprof/hprof_io.c
;; *
;; *

(def tag-name
  (into {}
        (map (fn [[k v]]
               [v k]))
        record-tags))

(def heap-dump-record-tags
  {   ;; ;; // data-dump sub-records
   :HPROF_GC_ROOT_UNKNOWN          0xFF
   :HPROF_GC_ROOT_JNI_GLOBAL       0x01
   :HPROF_GC_ROOT_JNI_LOCAL        0x02
   :HPROF_GC_ROOT_JAVA_FRAME       0x03
   :HPROF_GC_ROOT_NATIVE_STACK     0x04
   :HPROF_GC_ROOT_STICKY_CLASS     0x05
   :HPROF_GC_ROOT_THREAD_BLOCK     0x06
   :HPROF_GC_ROOT_MONITOR_USED     0x07
   :HPROF_GC_ROOT_THREAD_OBJ       0x08
   :HPROF_GC_CLASS_DUMP            0x20
   :HPROF_GC_INSTANCE_DUMP         0x21
   :HPROF_GC_OBJ_ARRAY_DUMP        0x22
   :HPROF_GC_PRIM_ARRAY_DUMP       0x23})

(def heap-dump-tag-name
  (into {}
        (map (fn [[k v]]
               [v k]))
        heap-dump-record-tags))

(def field-types
  {
   :HPROF_ARRAY_OBJECT             0x01
   :HPROF_NORMAL_OBJECT            0x02
   :HPROF_BOOLEAN                  0x04
   :HPROF_CHAR                     0x05
   :HPROF_FLOAT                    0x06
   :HPROF_DOUBLE                   0x07
   :HPROF_BYTE                     0x08
   :HPROF_SHORT                    0x09
   :HPROF_INT                      0x0A
   :HPROF_LONG                     0x0B   
   })
(def field-name
  (into {}
        (map (fn [[k v]]
               [v k]))
        field-types))

(defmacro rfn [k [input & args] & body]
  `(let [k# ~k]
     {:f (fn [~input ~@args]
           ~@body)
      :k k#}))

(defn read-bytes [is n]
  (let [buf (byte-array n)]
    (loop [offset 0]
      (if (< offset n)
        (let [bytes-read (.read ^InputStream is buf offset (- n offset))]
          (assert (pos? bytes-read))
          (recur (+ offset bytes-read)))
        [is buf]))))

(def inputstream-readers
  (into
   {}
   (map (juxt :k identity))
   [(rfn ::uint8 [is]
      [is (.read ^InputStream is)])

    (rfn ::int8 [is]
      (let [a (.read ^InputStream is)
            
            pos? (zero? (bit-and a
                                 0x80))
            num (if pos?
                  a
                  (- a))]
        [is num]))

    (rfn ::eof? [is]
      (let [next-byte (.read ^InputStream is)
            eof? (= -1 next-byte)]
        (.unread ^PushbackInputStream is next-byte)
        [is eof?]))

    (rfn ::uint16 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)]
        [is (bit-or (bit-shift-left a 8)
                    b)]))

    (rfn ::int16 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)

            num (bit-or (bit-shift-left a 8)
                        b)

            pos? (zero? (bit-and a
                                 0x80))
            num (if pos?
                  num
                  (- num))]
        [is num]))
    
    (rfn ::uint32 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)]
        [is
         (bit-or (bit-shift-left a 24)
                 (bit-shift-left b 16)
                 (bit-shift-left c 8)
                 d)]))

    (rfn ::float32 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)]
        [is
         (Float/intBitsToFloat
          (.intValue
           (bit-or (bit-shift-left a 24)
                   (bit-shift-left b 16)
                   (bit-shift-left c 8)
                   d)))]))


    (rfn ::int32 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)
            
            num (bit-or (bit-shift-left a 24)
                        (bit-shift-left b 16)
                        (bit-shift-left c 8)
                        d)

            pos? (zero? (bit-and a
                                 0x80000000))
            num (if pos?
                  num
                  (- num))]
        [is num]))

    (rfn ::uint64 [is]
      (let [[is magnitude] (read-bytes is 8)]
        [is
         (BigInteger. 1 ;; positive
                      magnitude)]))

    (rfn ::int64 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)
            e (.read ^InputStream is)
            f (.read ^InputStream is)
            g (.read ^InputStream is)
            h (.read ^InputStream is)]
        [is
         (bit-or
          (bit-shift-left a 56)
          (bit-shift-left b 48)
          (bit-shift-left c 40)
          (bit-shift-left d 32)
          (bit-shift-left e 24)
          (bit-shift-left f 16)
          (bit-shift-left g 8)
          h)]))

    (rfn ::float64 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)
            e (.read ^InputStream is)
            f (.read ^InputStream is)
            g (.read ^InputStream is)
            h (.read ^InputStream is)]
        [is
         (Double/longBitsToDouble
          (bit-or
           (bit-shift-left a 56)
           (bit-shift-left b 48)
           (bit-shift-left c 40)
           (bit-shift-left d 32)
           (bit-shift-left e 24)
           (bit-shift-left f 16)
           (bit-shift-left g 8)
           h))]))

    (rfn ::c-string [is]
      (loop [bs []]
        (let [b (.read ^InputStream is)]
          (if (zero? b)
            [is (String. (byte-array bs) "ascii")]
            (if (= -1 b)
              (throw (Exception. "Unterminated c-string."))
              (recur (conj bs b)))))))

    (rfn ::bytes [is n]
      (read-bytes is n))]))


(defn struct-parser* [name args fields]
  (let [rf## (gensym "rf_")
        sink## (gensym "sink_")
        source## (gensym "source_")
        parse-fn## (gensym "parse-fn_")
        bindings (into []
                       (comp
                        (partition-all 2)
                        (mapcat
                         (fn [[k type]]
                           (if (symbol? k)
                             ;; assume single output
                             [[k source##] `(~parse-fn## #(do %2) nil ~source## ~type)
                              sink## `(~rf## ~sink## ~(keyword k))
                              sink## `(~rf## ~sink## ~k)]
                             ;; else
                             [sink## `(~rf## ~sink## ~k)
                              [sink## source##] `(~parse-fn## ~rf## ~sink## ~source## ~type)])))
                        (partition-all 2)
                        #_(interpose [(gensym "__") `(when (reduced? ~sink##)
                                                       (throw (Exception. "Reduced!")))])
                        cat)
                       
                       fields)]
    `(fn [~parse-fn##]
       (fn [~rf## ~sink## ~source## ~@args]
         (let [~sink## (~rf## ~sink## [:begin ~name])]
           (let ~bindings
             (let [~sink## (~rf## ~sink## [:end ~name])]
               ~[sink## source##])))))))


(defmacro struct-parser [name args fields]
  (struct-parser* name args fields))

(def struct-parsers (atom {}))

(defmacro def-struct-parser [name args fields]
  `(let [name# ~name
         parser# (struct-parser name# ~args ~fields)]
     (swap! struct-parsers assoc name# parser#)
     nil))

(def-struct-parser
  ::hprof
  []
  [ ;; ::struct ;; :file-header 
   :version ::c-string
   identifier-size ::uint32

   :high-word ::uint32
   ;; * u4         low word    number of milliseconds since 0:00 GMT, 1/1/70
   :low-word ::uint32
   ;; * [record]*  a sequence of records.
   :records [:+
             [::record
              (case identifier-size
                4 ::int32
                8 ::int64)]]])


(def-struct-parser
  ::record
  [id-type]
  [tag ::uint8
   :microseconds ::uint32
   remaining-bytes ::uint32
   :body [::record-body id-type tag remaining-bytes]])

(defn parse-record-body [parse-fn]
  (fn [rf sink source id-type tag remaining-bytes]
    (let [type (get tag-name tag)]
      (parse-fn rf sink source [type id-type remaining-bytes]))))


(swap! struct-parsers
       assoc ::record-body parse-record-body)


;; * HPROF_UTF8               a UTF8-encoded name
;; *
;; *               id         name ID
;; *               [u1]*      UTF8 characters (no trailing zero)
(def-struct-parser
  :HPROF_UTF8
  [id-type remaining-bytes]
  [:id id-type
   :str [::tag-string id-type remaining-bytes]])

;; * HPROF_LOAD_CLASS         a newly loaded class
;; *
;; *                u4        class serial number (> 0)
;; *                id        class object ID
;; *                u4        stack trace serial number
;; *                id        class name ID
(def-struct-parser
  :HPROF_LOAD_CLASS ;; a newly loaded class
  [id-type remaining-size]
  [:class-serial-number ::uint32
   :object-id id-type
   :stacktrace-serial-number ::uint32
   :class-name-id id-type])



;; * HPROF_UNLOAD_CLASS       an unloading class
;; *
;; *                u4        class serial_number
(def-struct-parser
  :HPROF_UNLOAD_CLASS ;; an unloading class
  [id-type remaining-size]
  [:class-serial-number ::uint32])

;; * HPROF_FRAME              a Java stack frame
;; *
;; *                id        stack frame ID
;; *                id        method name ID
;; *                id        method signature ID
;; *                id        source file name ID
;; *                u4        class serial number
;; *                i4        line number. >0: normal
;; *                                       -1: unknown
;; *                                       -2: compiled method
;; *                                       -3: native method
(def-struct-parser
  :HPROF_FRAME ;; a Java stack trace
  [id-type remaining-size]
  [:stack-frame-id id-type
   :method-name-id id-type
   :method-signature-id id-type
   :source-file-name-id id-type
   :serial-number ::uint32
   :line-number ::int32])


;; * HPROF_TRACE              a Java stack trace
;; *
;; *               u4         stack trace serial number
;; *               u4         thread serial number
;; *               u4         number of frames
;; *               [id]*      stack frame IDs
(def-struct-parser
  :HPROF_TRACE ;; a Java stack trace
  [id-type remaining-size]
  [:stacktrace-serial-number ::uint32
   :thread-serial-number ::uint32
   num-frames ::uint32
   :frame-ids [::n num-frames id-type]])

;; * HPROF_ALLOC_SITES        a set of heap allocation sites, obtained after GC
;; *
;; *               u2         flags 0x0001: incremental vs. complete
;; *                                0x0002: sorted by allocation vs. live
;; *                                0x0004: whether to force a GC
;; *               u4         cutoff ratio
;; *               u4         total live bytes
;; *               u4         total live instances
;; *               u8         total bytes allocated
;; *               u8         total instances allocated
;; *               u4         number of sites that follow
;; *               [u1        is_array: 0:  normal object
;; *                                    2:  object array
;; *                                    4:  boolean array
;; *                                    5:  char array
;; *                                    6:  float array
;; *                                    7:  double array
;; *                                    8:  byte array
;; *                                    9:  short array
;; *                                    10: int array
;; *                                    11: long array
;; *                u4        class serial number (may be zero during startup)
;; *                u4        stack trace serial number
;; *                u4        number of bytes alive
;; *                u4        number of instances alive
;; *                u4        number of bytes allocated
;; *                u4]*      number of instance allocated
(def-struct-parser
  :HPROF_ALLOC_SITES ;; a set of heap allocation sites, obtained after GC
  [id-type remaining-size]
  [:flags ::uint16
   :cutoff-ratio ::uint32
   :total-live-bytes ::uint32         
   :total-live-instances ::uint32
   :total-bytes-allocated ::uint64
   :total-instances-allocated ::uint64
   num-sites ::uint32
   
   :sites
   [::n num-sites
    [:is-array ::uint8
     :class-serial-number ::uint32
     :stacktrace-serial-number ::uint32
     :number-of-bytes-alive ::uint32
     :number-of-instances-alive ::uint32
     :number-of-bytes-allocated ::uint32
     :number-of-instances-allocated ::uint32]]])

;; * HPROF_START_THREAD       a newly started thread.
;; *
;; *               u4         thread serial number (> 0)
;; *               id         thread object ID
;; *               u4         stack trace serial number
;; *               id         thread name ID
;; *               id         thread group name ID
;; *               id         thread group parent name ID
(def-struct-parser
  :HPROF_START_THREAD ;; a newly started thread.
  [id-type remaining-size]
  [:thread-serial-number ::uint32
   :thread-object-id id-type
   :stacktrace-serial-number ::uint32
   :thread-name-id id-type
   :thread-group-name-id id-type
   :thread-group-parent-name-id id-type])

;; * HPROF_END_THREAD         a terminating thread.
;; *
;; *               u4         thread serial number
(def-struct-parser
  :HPROF_END_THREAD ;; a terminating thread.
  [id-type remaining-size]
  [:thread-serial-number ::uint32])

;; * HPROF_HEAP_SUMMARY       heap summary
;; *
;; *               u4         total live bytes
;; *               u4         total live instances
;; *               u8         total bytes allocated
;; *               u8         total instances allocated
(def-struct-parser
  :HPROF_HEAP_SUMMARY ;; heap summary
  [id-type remaining-size]
  [:total-live-bytes ::uint32
   :total-live-instances ::uint32
   :total-bytes-allocated ::uint64
   :total-instances-allocated ::uint64])


(def-struct-parser
  :HPROF_GC_ROOT_UNKNOWN
  [id-type]
  [:object-id id-type])

(def-struct-parser
  :HPROF_GC_ROOT_THREAD_OBJ
  [id-type]
  [:thread-object-id id-type
   :thread-sequence-number ::uint32
   :stacktrace-sequence-number ::uint32])



(def-struct-parser
  :HPROF_GC_ROOT_JNI_GLOBAL ;; JNI global ref root
  [id-type]
  [:object-id id-type
   :jni-global-ref-id id-type])


(def-struct-parser
  :HPROF_GC_ROOT_JNI_LOCAL ;; JNI local ref
  [id-type]
  [:object-id id-type
   :thread-serial-number ::uint32
   :frame-number ::uint32])

(def-struct-parser
  :HPROF_GC_ROOT_JAVA_FRAME ;; Java stack frame
  [id-type]
  [:object-id id-type
   :thread-serial-number ::uint32
   :frame-number ::uint32])

(def-struct-parser
  :HPROF_GC_ROOT_NATIVE_STACK
  [id-type]
  [:object-id id-type
   :thread-serial-number ::uint32])

(def-struct-parser
  :HPROF_GC_ROOT_STICKY_CLASS ;; System class
  [id-type]
  [:object-id id-type])

(def-struct-parser
  :HPROF_GC_ROOT_THREAD_BLOCK ;; Reference from thread block
  [id-type]
  [:object-id id-type
   :thread-serial-number ::uint32])

(def-struct-parser
  :HPROF_GC_ROOT_MONITOR_USED ;; Busy monitor
  [id-type]
  [:object-id id-type])

(def-struct-parser
  :HPROF_GC_CLASS_DUMP
  [id-type]
  [
   ;; id         class object ID
   :class-object-id id-type
   ;; u4         stack trace serial number
   :stacktrace-serial-number ::uint32
   ;; id         super class object ID
   :super-class-object-id id-type
   ;; id         class loader object ID
   :loader-object-id id-type
   ;; id         signers object ID
   :signers-object-id id-type
   ;; id         protection domain object ID
   :protection-domain-object-id id-type
   ;; id         reserved
   :reserved id-type
   ;; id         reserved
   :reserved id-type

   ;; u4         instance size (in bytes)
   :instance-bytes-size ::uint32

   ;; u2         size of constant pool
   constant-pool-size ::uint16
   ;; AFAIK, constant-pool-size is always 0
   :constant-pool [::n constant-pool-size ::constant-pool-item]
   ;; [u2,       constant pool index,
   ;;  ty,       type
   ;;            2:  object
   ;;            4:  boolean
   ;;            5:  char
   ;;            6:  float
   ;;            7:  double
   ;;            8:  byte
   ;;            9:  short
   ;;            10: int
   ;;            11: long
   ;;  vl]*      and value


   static-field-count ::uint16
   :static-fields [::n static-field-count [::static-field id-type]]
   ;; u2         number of static fields
   ;; [id,       static field name,
   ;;  ty,       type,
   ;;  vl]*      and value

   ;; u2         number of inst. fields (not inc. super)
   instance-field-count ::uint16
   ;; [id,       instance field name,
   ;;  ty]*      type
   :instance-fields [::n instance-field-count [::instance-field id-type]]])


;; HPROF_GC_INSTANCE_DUMP        dump of a normal object

;;            id         object ID
;;            u4         stack trace serial number
;;            id         class object ID
;;            u4         number of bytes that follow
;;            [vl]*      instance field values (class, followed
;;                       by super, super's super ...)
(def-struct-parser
  :HPROF_GC_INSTANCE_DUMP ;; dump of a normal object
  [id-type]
  [:object-id id-type
   :stacktrace-serial-number ::uint32
   :class-object-id id-type
   instance-size ::uint32
   :instance-values [::bytes instance-size]])

;; 

;;            id         array object ID
;;            u4         stack trace serial number
;;            u4         number of elements
;;            id         array class ID
;;            [id]*      elements
(def-struct-parser
  :HPROF_GC_OBJ_ARRAY_DUMP ;; dump of an object array
  [id-type]
  [:object-id id-type
   :stacktrace-serial-number ::uint32
   num-elements ::uint32
   :array-class-id id-type
   :objects [::n num-elements id-type]])

;; HPROF_GC_PRIM_ARRAY_DUMP      dump of a primitive array

;;            id         array object ID
;;            u4         stack trace serial number
;;            u4         number of elements
;;            u1         element type
;;                       4:  boolean array
;;                       5:  char array
;;                       6:  float array
;;                       7:  double array
;;                       8:  byte array
;;                       9:  short array
;;                       10: int array
;;                       11: long array
;;            [u1]*      elements

(do
  (defn parse-primitive-array [parse-fn]
    (fn [rf sink source element-type num-elements id-type]
      (let [type-name (get field-name element-type)]
        (parse-fn rf sink source [::n num-elements [type-name id-type]]))))
  
  (swap! struct-parsers
         assoc ::primitive-array parse-primitive-array))

(def-struct-parser
  :HPROF_GC_PRIM_ARRAY_DUMP ;; dump of a primitive array
  [id-type]
  [:object-id id-type
   :stacktrace-serial-number ::uint32
   num-elements ::uint32
   element-type ::uint8
   :arr [::primitive-array element-type num-elements id-type]])




;; * HPROF_CPU_SAMPLES        a set of sample traces of running threads
;; *
;; *                u4        total number of samples
;; *                u4        # of traces
;; *               [u4        # of samples
;; *                u4]*      stack trace serial number
#_unused?

;; * HPROF_CONTROL_SETTINGS   the settings of on/off switches
;; *
;; *                u4        0x00000001: alloc traces on/off
;; *                          0x00000002: cpu sampling on/off
;; *                u2        stack trace depth
#_unused?

(do
  (defn parse-heap-dump-segment [parse-fn]
    (fn [rf sink source id-type remaining-bytes]
      (let [[buf source] (parse-fn #(do %2) nil source [::bytes remaining-bytes])
            sub-source (PushbackInputStream. (ByteArrayInputStream. buf) 1)
            sink (rf sink [:begin :HPROF_HEAP_DUMP_SEGMENT])
            sink (rf sink :records)
            [sink _] (parse-fn rf sink sub-source [:+
                                                   [::heap-dump-record id-type]])
            sink (rf sink [:end :HPROF_HEAP_DUMP_SEGMENT])]
        [sink source])))
  
  (swap! struct-parsers
         assoc :HPROF_HEAP_DUMP_SEGMENT parse-heap-dump-segment))


(def-struct-parser
  ::heap-dump-record
  [id-type]
  [subrecord-type ::uint8
   :heap-dump-body [::heap-dump-body id-type subrecord-type]])

(def-struct-parser
  :HPROF_HEAP_DUMP_END ;; a Java stack trace
  [id-type remaining-size]
  [])

(do
  ;; helper for debugging
  (defn parse-rest [parse-fn]
    (fn [rf sink source]
      (loop [n 0]
        (let [[byte source] (parse-fn #(do %2) nil source ::uint8)]
          (if (= -1 byte)
            (do (prn n)
                [sink source])
            (recur (inc n)))))))
  
  (swap! struct-parsers
         assoc ::parse-rest parse-rest))




(do
  (defn parse-n [parse-fn]
    (fn [rf sink source n type]
      (let [sink (rf sink ::start-vec)]
        (loop [[sink source] [sink source]
               n n]
          (if (pos? n)
            (let [[sink source :as result] (parse-fn rf sink source type)]
              (if (reduced? sink)
                result
                (recur result (dec n))))
            (let [sink (rf sink ::end)]
              [sink source]))))))
  
  (swap! struct-parsers
         assoc ::n parse-n))


(do
  (defn parse-static-field-value [parse-fn]
    (fn [rf sink source id-type field-type]
      (let [type-name (get field-name field-type)]
        (assert type-name (str "Unknown field type: " field-type))
        (parse-fn rf sink source [type-name id-type]))))
  
  (swap! struct-parsers
         assoc ::static-field-value parse-static-field-value))

(def-struct-parser :HPROF_ARRAY_OBJECT
  [id-type]
  [:object-id id-type])

(def-struct-parser :HPROF_NORMAL_OBJECT
  [id-type]
  [:object-id id-type])

(do
  (defn parse-field-value-bool [parse-fn]
    (fn [rf sink source id-type]
      (let [[bool source] (parse-fn #(do %2) nil source ::uint8)
            sink (rf sink (zero? bool))]
        [sink source])))
  
  (swap! struct-parsers
         assoc :HPROF_BOOLEAN parse-field-value-bool))


(do
  (defn parse-field-value-char [parse-fn]
    (fn [rf sink source id-type]
      (let [[c source] (parse-fn #(do %2) nil source ::uint16)
            sink (rf sink (char c))]
        [sink source])))
  
  (swap! struct-parsers
         assoc :HPROF_CHAR parse-field-value-char))


(def-struct-parser :HPROF_FLOAT
  [id-type]
  [:float-value ::float32])
(def-struct-parser :HPROF_DOUBLE
  [id-type]
  [:float-value ::float64])

(def-struct-parser :HPROF_BYTE
  [id-type]
  [:byte-value ::int8])
(def-struct-parser :HPROF_SHORT
  [id-type]
  [:short-value ::int16])
(def-struct-parser :HPROF_INT
  [id-type]
  [:int-value ::int32])
(def-struct-parser :HPROF_LONG
  [id-type]
  [:long-value ::int64])

(def-struct-parser
  ::static-field
  [id-type]
  [:static-field-name id-type
   static-field-type ::uint8
   :static-field-value [::static-field-value id-type static-field-type]])

(def-struct-parser
  ::instance-field
  [id-type]
  [:instance-field-name id-type
   :instance-field-type ::uint8])



(do
  (defn parse-heap-dump-body [parse-fn]
    (fn [rf sink source id-type subrecord-type]
      (let [type-name (get heap-dump-tag-name subrecord-type)]
        (assert type-name (str "Unknown heap dump subrecord type: " subrecord-type))
        (parse-fn rf sink source [type-name id-type]))))
  
  (swap! struct-parsers
         assoc ::heap-dump-body parse-heap-dump-body))


(defn parse-one-or-more [parse-fn]
  (fn [rf sink source type]
    (let [sink (rf sink ::start-vec)]
      (loop [[sink source] [sink source]]
        (let [[eof? source] (parse-fn #(do %2) nil source ::eof?)]
          (if eof?
            (let [sink (rf sink ::end)]
              [sink source])
            (let [[sink source :as result] (parse-fn rf sink source type)]
              (if (reduced? sink)
                result
                (recur result)))))))))

(swap! struct-parsers
       assoc :+ parse-one-or-more)


(defn parse-tag-string [parse-fn]
  (fn [rf sink source id-type remaining-bytes]
    (let [[bytes source] (parse-fn #(do %2) nil source [::bytes (- remaining-bytes
                                                                   (case id-type
                                                                     ::int32 4
                                                                     ::int64 8))])]
      [(rf sink (String. bytes "utf-8")) source])))


(swap! struct-parsers
       assoc ::tag-string parse-tag-string)





;; (defn parse-identifier [parse-fn]
;;   (fn [rf sink source identifier-size]
;;     (case identifier-size
;;       4 (parse-fn rf sink source ::uint32)
;;       8 (parse-fn rf sink source ::int64))))


;; (swap! struct-parsers
;;        assoc ::identifier parse-identifier)



(defn parse!
  ([xform f fname]
   (parse! xform f (f) fname))
  ([xform f init fname]
   (let [parser
         (fn parse-fn [rf sink source type & args]
           (cond
             (vector? type)
             (apply parse-fn rf sink source type)

             (contains? inputstream-readers type)
             (let [{:keys [f]} (get inputstream-readers type)
                   [source x] (apply f source args)]
               (let [sink (rf sink x)]
                 [sink source]))

             (contains? @struct-parsers type)
             (let [f (get @struct-parsers type)]
               (try
                 (apply (f parse-fn) rf sink source args)
                 (catch Exception e
                   (println type)
                   (throw e))))

             :else
             (throw (ex-info (str "Unknown type: " type)
                             {:type type
                              :args args}))))
         rf (xform f)]
     (let [[result _]
           (with-open [is (io/input-stream fname)
                       pbis (PushbackInputStream. is 1)]
             (parser rf init pbis ::hprof))]
       (f (unreduced result))))))

(comment
  (parse! (comp (take 30)) conj [] "mem.hprof" ::hprof)
  ,)


;; *  header    "JAVA PROFILE 1.0.2" (0-terminated)
;; *
;; *  u4        size of identifiers. Identifiers are used to represent
;; *            UTF8 strings objects, stack traces, etc. They usually
;; *            have the same size as host pointers. For example, on
;; *            Solaris and Win32, the size is 4.
;; * u4         high word
;; * u4         low word    number of milliseconds since 0:00 GMT, 1/1/70
;; * [record]*  a sequence of records.
;; *
;; *
;; * Record format:
;; *
;; * u1         a TAG denoting the type of the record
;; * u4         number of *microseconds* since the time stamp in the
;; *            header. (wraps around in a little more than an hour)
;; * u4         number of bytes *remaining* in the record. Note that
;; *            this number excludes the tag and the length field itself.
;; * [u1]*      BODY of the record (a sequence of bytes)
;; *
;; *
;; * The following TAGs are supported:
;; *
;; * TAG           BODY       notes
;; *----------------------------------------------------------
;; * HPROF_UTF8               a UTF8-encoded name
;; *
;; *               id         name ID
;; *               [u1]*      UTF8 characters (no trailing zero)
;; *
;; * HPROF_LOAD_CLASS         a newly loaded class
;; *
;; *                u4        class serial number (> 0)
;; *                id        class object ID
;; *                u4        stack trace serial number
;; *                id        class name ID
;; *
;; * HPROF_UNLOAD_CLASS       an unloading class
;; *
;; *                u4        class serial_number
;; *
;; * HPROF_FRAME              a Java stack frame
;; *
;; *                id        stack frame ID
;; *                id        method name ID
;; *                id        method signature ID
;; *                id        source file name ID
;; *                u4        class serial number
;; *                i4        line number. >0: normal
;; *                                       -1: unknown
;; *                                       -2: compiled method
;; *                                       -3: native method
;; *
;; * HPROF_TRACE              a Java stack trace
;; *
;; *               u4         stack trace serial number
;; *               u4         thread serial number
;; *               u4         number of frames
;; *               [id]*      stack frame IDs
;; *
;; *
;; * HPROF_ALLOC_SITES        a set of heap allocation sites, obtained after GC
;; *
;; *               u2         flags 0x0001: incremental vs. complete
;; *                                0x0002: sorted by allocation vs. live
;; *                                0x0004: whether to force a GC
;; *               u4         cutoff ratio
;; *               u4         total live bytes
;; *               u4         total live instances
;; *               u8         total bytes allocated
;; *               u8         total instances allocated
;; *               u4         number of sites that follow
;; *               [u1        is_array: 0:  normal object
;; *                                    2:  object array
;; *                                    4:  boolean array
;; *                                    5:  char array
;; *                                    6:  float array
;; *                                    7:  double array
;; *                                    8:  byte array
;; *                                    9:  short array
;; *                                    10: int array
;; *                                    11: long array
;; *                u4        class serial number (may be zero during startup)
;; *                u4        stack trace serial number
;; *                u4        number of bytes alive
;; *                u4        number of instances alive
;; *                u4        number of bytes allocated
;; *                u4]*      number of instance allocated
;; *
;; * HPROF_START_THREAD       a newly started thread.
;; *
;; *               u4         thread serial number (> 0)
;; *               id         thread object ID
;; *               u4         stack trace serial number
;; *               id         thread name ID
;; *               id         thread group name ID
;; *               id         thread group parent name ID
;; *
;; * HPROF_END_THREAD         a terminating thread.
;; *
;; *               u4         thread serial number
;; *
;; * HPROF_HEAP_SUMMARY       heap summary
;; *
;; *               u4         total live bytes
;; *               u4         total live instances
;; *               u8         total bytes allocated
;; *               u8         total instances allocated
;; *
;; * HPROF_HEAP_DUMP          denote a heap dump
;; *
;; *               [heap dump sub-records]*
;; *
;; *                          There are four kinds of heap dump sub-records:
;; *
;; *               u1         sub-record type
;; *
;; *               HPROF_GC_ROOT_UNKNOWN         unknown root
;; *
;; *                          id         object ID
;; *
;; *               HPROF_GC_ROOT_THREAD_OBJ      thread object
;; *
;; *                          id         thread object ID  (may be 0 for a
;; *                                     thread newly attached through JNI)
;; *                          u4         thread sequence number
;; *                          u4         stack trace sequence number
;; *
;; *               HPROF_GC_ROOT_JNI_GLOBAL      JNI global ref root
;; *
;; *                          id         object ID
;; *                          id         JNI global ref ID
;; *
;; *               HPROF_GC_ROOT_JNI_LOCAL       JNI local ref
;; *
;; *                          id         object ID
;; *                          u4         thread serial number
;; *                          u4         frame # in stack trace (-1 for empty)
;; *
;; *               HPROF_GC_ROOT_JAVA_FRAME      Java stack frame
;; *
;; *                          id         object ID
;; *                          u4         thread serial number
;; *                          u4         frame # in stack trace (-1 for empty)
;; *
;; *               HPROF_GC_ROOT_NATIVE_STACK    Native stack
;; *
;; *                          id         object ID
;; *                          u4         thread serial number
;; *
;; *               HPROF_GC_ROOT_STICKY_CLASS    System class
;; *
;; *                          id         object ID
;; *
;; *               HPROF_GC_ROOT_THREAD_BLOCK    Reference from thread block
;; *
;; *                          id         object ID
;; *                          u4         thread serial number
;; *
;; *               HPROF_GC_ROOT_MONITOR_USED    Busy monitor
;; *
;; *                          id         object ID
;; *
;; *               HPROF_GC_CLASS_DUMP           dump of a class object
;; *
;; *                          id         class object ID
;; *                          u4         stack trace serial number
;; *                          id         super class object ID
;; *                          id         class loader object ID
;; *                          id         signers object ID
;; *                          id         protection domain object ID
;; *                          id         reserved
;; *                          id         reserved
;; *
;; *                          u4         instance size (in bytes)
;; *
;; *                          u2         size of constant pool
;; *                          [u2,       constant pool index,
;; *                           ty,       type
;; *                                     2:  object
;; *                                     4:  boolean
;; *                                     5:  char
;; *                                     6:  float
;; *                                     7:  double
;; *                                     8:  byte
;; *                                     9:  short
;; *                                     10: int
;; *                                     11: long
;; *                           vl]*      and value
;; *
;; *                          u2         number of static fields
;; *                          [id,       static field name,
;; *                           ty,       type,
;; *                           vl]*      and value
;; *
;; *                          u2         number of inst. fields (not inc. super)
;; *                          [id,       instance field name,
;; *                           ty]*      type
;; *
;; *               HPROF_GC_INSTANCE_DUMP        dump of a normal object
;; *
;; *                          id         object ID
;; *                          u4         stack trace serial number
;; *                          id         class object ID
;; *                          u4         number of bytes that follow
;; *                          [vl]*      instance field values (class, followed
;; *                                     by super, super's super ...)
;; *
;; *               HPROF_GC_OBJ_ARRAY_DUMP       dump of an object array
;; *
;; *                          id         array object ID
;; *                          u4         stack trace serial number
;; *                          u4         number of elements
;; *                          id         array class ID
;; *                          [id]*      elements
;; *
;; *               HPROF_GC_PRIM_ARRAY_DUMP      dump of a primitive array
;; *
;; *                          id         array object ID
;; *                          u4         stack trace serial number
;; *                          u4         number of elements
;; *                          u1         element type
;; *                                     4:  boolean array
;; *                                     5:  char array
;; *                                     6:  float array
;; *                                     7:  double array
;; *                                     8:  byte array
;; *                                     9:  short array
;; *                                     10: int array
;; *                                     11: long array
;; *                          [u1]*      elements
;; *
;; * HPROF_CPU_SAMPLES        a set of sample traces of running threads
;; *
;; *                u4        total number of samples
;; *                u4        # of traces
;; *               [u4        # of samples
;; *                u4]*      stack trace serial number
;; *
;; * HPROF_CONTROL_SETTINGS   the settings of on/off switches
;; *
;; *                u4        0x00000001: alloc traces on/off
;; *                          0x00000002: cpu sampling on/off
;; *                u2        stack trace depth
;; *
;; *
;; * When the header is "JAVA PROFILE 1.0.2" a heap dump can optionally
;; * be generated as a sequence of heap dump segments. This sequence is
;; * terminated by an end record. The additional tags allowed by format
;; * "JAVA PROFILE 1.0.2" are:
;; *
;; * HPROF_HEAP_DUMP_SEGMENT  denote a heap dump segment
;; *
;; *               [heap dump sub-records]*
;; *               The same sub-record types allowed by HPROF_HEAP_DUMP
;; *
;; * HPROF_HEAP_DUMP_END      denotes the end of a heap dump
;; *
;; */



(defn heap-dump [output-path live?]
  (let [b ^HotSpotDiagnosticMXBean
        (ManagementFactory/newPlatformMXBeanProxy
         (ManagementFactory/getPlatformMBeanServer)
         "com.sun.management:type=HotSpotDiagnostic"
         HotSpotDiagnosticMXBean)]
    (.dumpHeap b output-path live?)))



;; helper for processing outpu
(defn find-key [k]
  (let [ready? (volatile! false)
        pred (if (keyword? k)
               (set [k])
               k)]
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (if-let [k @ready?]
           (do (vreset! ready? false)
               (rf result input))
           (do
             (when (pred input)
               (vreset! ready? input))
             result)))))))


(defn find-struct [type]
  (let [ready? (volatile! false)]
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (if @ready?
           (if (= [:end type] input)
             (do (vreset! ready? false)
                 (rf result ::end))
             (rf result input))
           (if (= input [:begin type])
             (do (vreset! ready? true)
                 (rf result ::start-map))
             result)
           ))))))

(comment
  (let [xform
        (comp (find-key :instance-field-name))]
    (def tags (parse! xform conj []  "mem.hprof" ::hprof)))

  (let [xform
        (comp (find-key :super-class-object-id))]
    (def tags (parse! xform conj []  "mem.hprof" ::hprof)))
  ,
  )





(defn hydrate
  ([] nil)
  ([z] z)
  ([z token]
   (case token
     ::start-vec (collect/begin-vec z)
     ::start-map (collect/begin-map z)
     ::end (collect/end z)
     ;; else
     (cond
       (vector? token)
       (let [[event type] token]
         (case event
           :begin (-> z
                      (collect/begin-map)
                      (hydrate :type)
                      (hydrate type))
           :end (collect/end z)))

       :else
       (collect/append z token)))))

(defn ->string-map [fname]
  (let [xform
        (comp (find-struct :HPROF_UTF8))
        tags (parse! xform conj [] fname)
        strs (hydrate (reduce hydrate (hydrate nil ::start-vec) tags) ::end)]
    (into {}
          (map (fn [{:keys [id str]}]
                 [id str]))
          strs)))



(comment

  (def full-parse
    (parse! identity hydrate "mem.hprof"))

  (defn ->classes? [fname]
    (let [xform
          (comp (find-struct :HPROF_LOAD_CLASS))
          tags (parse! xform conj []  fname ::hprof)
          result (hydrate (reduce hydrate (hydrate nil ::start-vec) tags) ::end)]
      (into {}
            (map (fn [{:keys [object-id class-name-id]}]
                   [object-id (get my-str-map class-name-id)]))
            result)))

  (def kls (->classes? "mem.hprof"))

  (def my-str-map (->string-map "mem.hprof") )

  (def class-object-ids (parse! (find-key :class-object-id) conj [] "mem.hprof" ::hprof))
  (def class-names (map my-str-map class-object-ids))

  (->> (clojure.reflect/reflect Class)
       :members
       (filter #(instance? clojure.reflect.Field %))
       (map :name)
       )
  )
