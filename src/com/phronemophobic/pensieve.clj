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
        buf))))

(def inputstream-readers
  (into
   {}
   (map (juxt :k :f))
   [(rfn ::uint8 [is]
      (.read ^InputStream is))

    (rfn ::int8 [is]
      (let [a (.read ^InputStream is)
            
            pos? (zero? (bit-and a
                                 0x80))
            num (if pos?
                  a
                  (- a))]
        num))

    (rfn ::eof? [is]
      (let [next-byte (.read ^InputStream is)
            eof? (= -1 next-byte)]
        (.unread ^PushbackInputStream is next-byte)
        eof?))

    (rfn ::uint16 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)]
        (bit-or (bit-shift-left a 8)
                b)))

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
        num))
    
    (rfn ::uint32 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)]
        (bit-or (bit-shift-left a 24)
                (bit-shift-left b 16)
                (bit-shift-left c 8)
                d)))

    (rfn ::float32 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)]
        (Float/intBitsToFloat
         (.intValue
          (bit-or (bit-shift-left a 24)
                  (bit-shift-left b 16)
                  (bit-shift-left c 8)
                  d)))))


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
        num))

    (rfn ::uint64 [is]
      (let [magnitude (read-bytes is 8)]
        (BigInteger. 1 ;; positive
                     magnitude)))

    (rfn ::int64 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)
            e (.read ^InputStream is)
            f (.read ^InputStream is)
            g (.read ^InputStream is)
            h (.read ^InputStream is)]
        (bit-or
         (bit-shift-left a 56)
         (bit-shift-left b 48)
         (bit-shift-left c 40)
         (bit-shift-left d 32)
         (bit-shift-left e 24)
         (bit-shift-left f 16)
         (bit-shift-left g 8)
         h)))

    (rfn ::float64 [is]
      (let [a (.read ^InputStream is)
            b (.read ^InputStream is)
            c (.read ^InputStream is)
            d (.read ^InputStream is)
            e (.read ^InputStream is)
            f (.read ^InputStream is)
            g (.read ^InputStream is)
            h (.read ^InputStream is)]
        (Double/longBitsToDouble
         (bit-or
          (bit-shift-left a 56)
          (bit-shift-left b 48)
          (bit-shift-left c 40)
          (bit-shift-left d 32)
          (bit-shift-left e 24)
          (bit-shift-left f 16)
          (bit-shift-left g 8)
          h))))

    (rfn ::c-string [is]
      (loop [bs []]
        (let [b (.read ^InputStream is)]
          (if (zero? b)
            (String. (byte-array bs) "ascii")
            (if (= -1 b)
              (throw (Exception. "Unterminated c-string."))
              (recur (conj bs b)))))))

    (rfn ::bytes [is n]
      (read-bytes is n))]))


(defn struct-tokenizer* [name args fields]
  (let [tokenize!## (gensym "tokenize!_")
        read!## (gensym "read!_")
        write!## (gensym "write!_")
        bindings (into []
                       (comp
                        (partition-all 2)
                        (mapcat
                         (fn [[k type]]
                           (if (symbol? k)
                             ;; assume single input
                             [k `(let [val# (~read!## ~type)]
                                   (~write!## ~(keyword k))
                                   (~write!## val#)
                                   val#)]
                             ;; else
                             `[_# (do
                                    (~write!## ~k)
                                    (~tokenize!## ~type))])))
                        (partition-all 2)
                        #_(interpose [(gensym "__") `(when (reduced? ~sink##)
                                                       (throw (Exception. "Reduced!")))])
                        cat)
                       
                       fields)]
    `(fn [~tokenize!## ~read!## ~write!##]
       (fn [~@args]
         (~write!## [:begin ~name])
         (let ~bindings
           (~write!## [:end ~name]))))))


(defmacro struct-tokenizer [name args fields]
  (struct-tokenizer* name args fields))

(def tokenizers (atom {}))

(defmacro def-struct-tokenizer [name args fields]
  `(let [name# ~name
         tokenizer# (struct-tokenizer name# ~args ~fields)]
     (swap! tokenizers assoc name# tokenizer#)
     nil))

(defmacro deftokenizer [type args & body]
  `(do
     (swap!
      tokenizers
      assoc ~type
      (fn [~'tokenize! ~'read! ~'write!]
        (fn ~args
          ~@body)))
      nil))

(deftokenizer ::struct [fields]
  (write! ::start-map)
  (doseq [[k format] fields]
    (write! k)
    (tokenize! format))
  (write! ::end))

(def-struct-tokenizer
  ::hprof
  []
  [ ;; ::struct ;; :file-header 
   :version ::c-string
   identifier-size ::uint32

   :high-word ::uint32
   ;; * u4         low word    number of milliseconds since 0:00 GMT, 1/1/70
   :low-word ::uint32
   ;; * [record]*  a sequence of records.
   :records [::+
             [::record
              (case identifier-size
                4 ::int32
                8 ::int64)]]])


(def-struct-tokenizer
  ::record
  [id-type]
  [tag ::uint8
   :microseconds ::uint32
   remaining-bytes ::uint32
   :body [::record-body id-type tag remaining-bytes]])


(deftokenizer ::record-body [id-type tag remaining-bytes]
  (let [type (get tag-name tag)]
    (assert type)
    (tokenize! type id-type remaining-bytes)))


;; * HPROF_UTF8               a UTF8-encoded name
;; *
;; *               id         name ID
;; *               [u1]*      UTF8 characters (no trailing zero)
(def-struct-tokenizer
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
(def-struct-tokenizer
  :HPROF_LOAD_CLASS ;; a newly loaded class
  [id-type remaining-size]
  [:class-serial-number ::uint32
   :object-id id-type
   :stacktrace-serial-number ::uint32
   :class-name-id id-type])



;; * HPROF_UNLOAD_CLASS       an unloading class
;; *
;; *                u4        class serial_number
(def-struct-tokenizer
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
(def-struct-tokenizer
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
(def-struct-tokenizer
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
(def-struct-tokenizer
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
(def-struct-tokenizer
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
(def-struct-tokenizer
  :HPROF_END_THREAD ;; a terminating thread.
  [id-type remaining-size]
  [:thread-serial-number ::uint32])

;; * HPROF_HEAP_SUMMARY       heap summary
;; *
;; *               u4         total live bytes
;; *               u4         total live instances
;; *               u8         total bytes allocated
;; *               u8         total instances allocated
(def-struct-tokenizer
  :HPROF_HEAP_SUMMARY ;; heap summary
  [id-type remaining-size]
  [:total-live-bytes ::uint32
   :total-live-instances ::uint32
   :total-bytes-allocated ::uint64
   :total-instances-allocated ::uint64])


(def-struct-tokenizer
  :HPROF_GC_ROOT_UNKNOWN
  [id-type]
  [:object-id id-type])

(def-struct-tokenizer
  :HPROF_GC_ROOT_THREAD_OBJ
  [id-type]
  [:thread-object-id id-type
   :thread-sequence-number ::uint32
   :stacktrace-sequence-number ::uint32])



(def-struct-tokenizer
  :HPROF_GC_ROOT_JNI_GLOBAL ;; JNI global ref root
  [id-type]
  [:object-id id-type
   :jni-global-ref-id id-type])


(def-struct-tokenizer
  :HPROF_GC_ROOT_JNI_LOCAL ;; JNI local ref
  [id-type]
  [:object-id id-type
   :thread-serial-number ::uint32
   :frame-number ::uint32])

(def-struct-tokenizer
  :HPROF_GC_ROOT_JAVA_FRAME ;; Java stack frame
  [id-type]
  [:object-id id-type
   :thread-serial-number ::uint32
   :frame-number ::uint32])

(def-struct-tokenizer
  :HPROF_GC_ROOT_NATIVE_STACK
  [id-type]
  [:object-id id-type
   :thread-serial-number ::uint32])

(def-struct-tokenizer
  :HPROF_GC_ROOT_STICKY_CLASS ;; System class
  [id-type]
  [:object-id id-type])

(def-struct-tokenizer
  :HPROF_GC_ROOT_THREAD_BLOCK ;; Reference from thread block
  [id-type]
  [:object-id id-type
   :thread-serial-number ::uint32])

(def-struct-tokenizer
  :HPROF_GC_ROOT_MONITOR_USED ;; Busy monitor
  [id-type]
  [:object-id id-type])

(def-struct-tokenizer
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
(def-struct-tokenizer
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
(def-struct-tokenizer
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

(deftokenizer ::primitive-array [element-type num-elements id-type]
  (let [type-name (get field-name element-type)]
    (tokenize! ::n num-elements [type-name id-type])))


(def-struct-tokenizer
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

(deftokenizer :HPROF_HEAP_DUMP_SEGMENT [id-type remaining-bytes]
  (read! ::reset-read-count)
  (write! [:begin :HPROF_HEAP_DUMP_SEGMENT])
  (write! :records)
  (write! ::start-vec)
  (loop [remaining-bytes remaining-bytes]
    (cond
      (pos? remaining-bytes)
      (do
        #_(tokenize! ::heap-dump-record id-type)
        (tokenize! ::heap-dump-record id-type)
        (recur (- remaining-bytes
                  (read! ::reset-read-count))))

      (neg? remaining-bytes)
      (throw (ex-info "Offset mismatch when reading dump segment"
                      {:remaining-bytes remaining-bytes}))

      :else
      (do
        (write! ::end) ;; end vec
        (write! [:end :HPROF_HEAP_DUMP_SEGMENT])))))




(def-struct-tokenizer
  ::heap-dump-record
  [id-type]
  [subrecord-type ::uint8
   :heap-dump-body [::heap-dump-body id-type subrecord-type]])

(def-struct-tokenizer
  :HPROF_HEAP_DUMP_END ;; a Java stack trace
  [id-type remaining-size]
  [])


(deftokenizer ::n [n type]
  (write! ::start-vec)
  (loop [n n]
    (if (pos? n)
      (let [done? (tokenize! type)]
        (when (not done?)
          (recur (dec n))))
      (write! ::end))))


(deftokenizer ::static-field-value [id-type field-type]
  (let [type-name (get field-name field-type)]
    (assert type-name (str "Unknown field type: " field-type))
    (tokenize! type-name id-type)))



(def-struct-tokenizer :HPROF_ARRAY_OBJECT
  [id-type]
  [:object-id id-type])

(def-struct-tokenizer :HPROF_NORMAL_OBJECT
  [id-type]
  [:object-id id-type])

(deftokenizer :HPROF_BOOLEAN [id-type]
  (let [i (read! ::uint8)]
    (write! (zero? i))))

(deftokenizer :HPROF_CHAR [id-type]
  (let [c (read! ::uint16)]
    (write! (char c))))



(deftokenizer :HPROF_FLOAT [id-type]
  (tokenize! ::float32))

(deftokenizer :HPROF_DOUBLE [id-type]
  (tokenize! ::float64))

(deftokenizer :HPROF_BYTE [id-type]
  (tokenize! ::int8))

(deftokenizer :HPROF_SHORT [id-type]
  (tokenize! ::int16))

(deftokenizer :HPROF_INT [id-type]
  (tokenize! ::int32))

(deftokenizer :HPROF_LONG [id-type]
  (tokenize! ::int64))

(def-struct-tokenizer
  ::static-field
  [id-type]
  [:static-field-name id-type
   static-field-type ::uint8
   :static-field-value [::static-field-value id-type static-field-type]])

(def-struct-tokenizer
  ::instance-field
  [id-type]
  [:instance-field-name id-type
   :instance-field-type ::uint8])



(deftokenizer ::heap-dump-body [id-type subrecord-type]
  (let [type-name (get heap-dump-tag-name subrecord-type)]
    (assert type-name (str "Unknown heap dump subrecord type: " subrecord-type))
    (tokenize! type-name id-type)))

(deftokenizer ::+ [type]
  (write! ::start-vec)
  (loop []
    (let [eof? (read! ::eof?)]
      (if eof?
        (write! ::end)
        (do
          (tokenize! type)
          (recur))))))

(deftokenizer ::tag-string [id-type remaining-bytes]
  (let [buf (read! ::bytes (- remaining-bytes
                              (case id-type
                                ::int32 4
                                ::int64 8)))]
    (write! (String. buf "utf-8"))))



(def read-sizes
  {:com.phronemophobic.pensieve/float32 4
   :com.phronemophobic.pensieve/float64 8
   :com.phronemophobic.pensieve/int16 2
   :com.phronemophobic.pensieve/int32 4
   :com.phronemophobic.pensieve/int64 8
   :com.phronemophobic.pensieve/int8 1
   :com.phronemophobic.pensieve/uint16 2
   :com.phronemophobic.pensieve/uint32 4
   :com.phronemophobic.pensieve/uint64 8
   :com.phronemophobic.pensieve/uint8 1})

(defn wrap-read-count [read!]
  (let [read-count (volatile! 0)]
    (fn [type & args]
      (case type
        (:com.phronemophobic.pensieve/float32
         :com.phronemophobic.pensieve/float64
         :com.phronemophobic.pensieve/int16
         :com.phronemophobic.pensieve/int32
         :com.phronemophobic.pensieve/int64
         :com.phronemophobic.pensieve/int8
         :com.phronemophobic.pensieve/uint16
         :com.phronemophobic.pensieve/uint32
         :com.phronemophobic.pensieve/uint64
         :com.phronemophobic.pensieve/uint8)
        (do
          (vswap! read-count + (read-sizes type))
          (apply read! type args))

        :com.phronemophobic.pensieve/bytes
        (do
          (vswap! read-count + (first args))
          (apply read! type args))

        :com.phronemophobic.pensieve/c-string
        (let [ret (read! type)]
          (vswap! read-count + (count ret))
          ret)

        :com.phronemophobic.pensieve/eof? (read! type)
        :com.phronemophobic.pensieve/read-count @read-count
        :com.phronemophobic.pensieve/reset-read-count (let [old @read-count]
                                                        (vreset! read-count 0)
                                                        old)))))


(defn make-tokenizer [readers tokenizers]
  (fn ([xform f init input type & args]
     (let [rf (xform f)

           read! (fn [type & args]
                   (let [f (get readers type)]
                     (assert f (str "Unknown type: " type))
                     (apply f input args)))
           read! (wrap-read-count read!)

           ret (volatile! init)
           write! (fn [x]
                    (let [result (vswap! ret rf x)]
                      false
                      #_(reduced? result)))
           tokenize! (fn tokenize! [type & args]
                       (if (vector? type)
                         (apply tokenize! type)
                         (if-let [f (get tokenizers type)]
                           (let [f (f tokenize! read! write!)]
                             (apply f args))
                           (write! (apply read! type args)))))]
       (apply tokenize! type args)
       (rf @ret)))))

(defn tokenize-hprof [fname]
  (let [tokenizer (make-tokenizer inputstream-readers @tokenizers)]
    (with-open [is (io/input-stream fname)
                pbis (PushbackInputStream. is 1)]
      (persistent!
       (tokenizer identity conj! (transient []) pbis ::hprof)))))

(defn parse-instance-values [{:keys [id->str]}
                             classes
                             instance-values]
  (let [{:keys [instance-fields]} class
        id-type ::int64
        fields (->> (into []
                          (comp (mapcat :instance-fields)
                                (map
                                 (fn [{:keys [instance-field-name instance-field-type]}]
                                   [(keyword (id->str instance-field-name))
                                    [(get field-name instance-field-type) id-type]])))
                          classes))
        format [::struct fields]]
    (let [tokenizer (make-tokenizer inputstream-readers @tokenizers)]
      (with-open [is (ByteArrayInputStream. instance-values)
                  pbis (PushbackInputStream. is 1)]
        (let [result (tokenizer identity hydrate (hydrate) pbis format)

	      remaining (loop [n 0]
                          (let [b (.read pbis)]
                            (if (= -1 b)
                              n
                              (recur (inc n)))))]
          result)))))


(defn class-chain [classes class]
  (into []
        (take-while some?)
        (iterate #(get classes
                       (:super-class-object-id %))
                 class)))

(defn hydrate-instance [info instance]
  (let [{:keys [classes class->str]} info
        {:keys [instance-size
                class-object-id
                instance-values
                object-id]} instance]
    (merge
     {:class (class->str class-object-id)
      :data
      (parse-instance-values info
                             (class-chain classes
                                          (get classes class-object-id))
                             instance-values)}
     instance))
  
  )


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

(defn ->string-map [hprof]
  (into {}
        (comp
         (map :body)
         (filter #(= :HPROF_UTF8 (:type %)))
         (map (fn [{:keys [id str]}]
                [id str])))
        (:records hprof)))

(defn class-names [hprof id->str]
  (let [

        class-loads (->> hprof
                         :records
                         (map :body)
                         (filter #(= :HPROF_LOAD_CLASS (:type %))))]
    
    (into {}
          (map (fn [{:keys [object-id class-name-id]}]
                 [object-id (get id->str class-name-id)]))
          class-loads)))


(defn instance-defs [hprof]
  (->> hprof
       :records
       (map :body)
       (filter #(= :HPROF_HEAP_DUMP_SEGMENT (:type %)))
       (mapcat :records)
       (map :heap-dump-body)
       (filter #(= :HPROF_GC_INSTANCE_DUMP (:type %)))
       (into {} (map (juxt :object-id identity))))
  )




(defn class-defs [hprof]
  (->> hprof
       :records
       (map :body)
       (filter #(= :HPROF_HEAP_DUMP_SEGMENT (:type %)))
       (mapcat :records)
       (map :heap-dump-body)
       (filter #(= :HPROF_GC_CLASS_DUMP (:type %)))
       (into {} (map (juxt :class-object-id identity))))
  )


(defn hprof-info [fname]
  (let [parsed (reduce hydrate (hydrate) (tokenize-hprof fname))
        classes (class-defs parsed)
        id->str (->string-map parsed)
        
        class->str (class-names parsed id->str)
        instances (instance-defs parsed)
        _ (prn (count instances))
        info {:classes classes
              :id->str id->str
              :class->str class->str}

        instances (update-vals instances
                               #(hydrate-instance info %))]
    (assoc info
           :instances instances)))

(comment
  (def full-tokenize
    (tokenize! identity hydrate "mem.hprof"))
  (def cds (class-defs full-tokenize))
  (def id->str (->string-map full-tokenize))
  (def instances)
  ,)
