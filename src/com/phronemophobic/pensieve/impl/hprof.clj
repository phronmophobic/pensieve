(ns com.phronemophobic.pensieve.impl.hprof
  (:require [com.phronemophobic.pensieve.impl.tokenizer
             :refer [deftokenizer
                     def-struct-tokenizer]
             :as tokenizer]
            [clojure.java.io :as io])
  (:import java.io.ByteArrayInputStream
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




(def-struct-tokenizer
  ::hprof
  []
  [ ;; ::tokenizer/struct ;; :file-header 
   :version ::tokenizer/c-string
   identifier-size ::tokenizer/uint32

   :high-word ::tokenizer/uint32
   ;; * u4         low word    number of milliseconds since 0:00 GMT, 1/1/70
   :low-word ::tokenizer/uint32
   ;; * [record]*  a sequence of records.
   :records [::tokenizer/+
             [::record
              (case identifier-size
                4 ::tokenizer/int32
                8 ::tokenizer/int64)]]])


(def-struct-tokenizer
  ::record
  [id-type]
  [tag ::tokenizer/uint8
   :microseconds ::tokenizer/uint32
   remaining-bytes ::tokenizer/uint32
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
  [:class-serial-number ::tokenizer/uint32
   :object-id id-type
   :stacktrace-serial-number ::tokenizer/uint32
   :class-name-id id-type])



;; * HPROF_UNLOAD_CLASS       an unloading class
;; *
;; *                u4        class serial_number
(def-struct-tokenizer
  :HPROF_UNLOAD_CLASS ;; an unloading class
  [id-type remaining-size]
  [:class-serial-number ::tokenizer/uint32])

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
   :serial-number ::tokenizer/uint32
   :line-number ::tokenizer/int32])


;; * HPROF_TRACE              a Java stack trace
;; *
;; *               u4         stack trace serial number
;; *               u4         thread serial number
;; *               u4         number of frames
;; *               [id]*      stack frame IDs
(def-struct-tokenizer
  :HPROF_TRACE ;; a Java stack trace
  [id-type remaining-size]
  [:stacktrace-serial-number ::tokenizer/uint32
   :thread-serial-number ::tokenizer/uint32
   num-frames ::tokenizer/uint32
   :frame-ids [::tokenizer/n num-frames id-type]])

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
  [:flags ::tokenizer/uint16
   :cutoff-ratio ::tokenizer/uint32
   :total-live-bytes ::tokenizer/uint32         
   :total-live-instances ::tokenizer/uint32
   :total-bytes-allocated ::tokenizer/uint64
   :total-instances-allocated ::tokenizer/uint64
   num-sites ::tokenizer/uint32
   
   :sites
   [::tokenizer/n num-sites
    [:is-array ::tokenizer/uint8
     :class-serial-number ::tokenizer/uint32
     :stacktrace-serial-number ::tokenizer/uint32
     :number-of-bytes-alive ::tokenizer/uint32
     :number-of-instances-alive ::tokenizer/uint32
     :number-of-bytes-allocated ::tokenizer/uint32
     :number-of-instances-allocated ::tokenizer/uint32]]])

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
  [:thread-serial-number ::tokenizer/uint32
   :thread-object-id id-type
   :stacktrace-serial-number ::tokenizer/uint32
   :thread-name-id id-type
   :thread-group-name-id id-type
   :thread-group-parent-name-id id-type])

;; * HPROF_END_THREAD         a terminating thread.
;; *
;; *               u4         thread serial number
(def-struct-tokenizer
  :HPROF_END_THREAD ;; a terminating thread.
  [id-type remaining-size]
  [:thread-serial-number ::tokenizer/uint32])

;; * HPROF_HEAP_SUMMARY       heap summary
;; *
;; *               u4         total live bytes
;; *               u4         total live instances
;; *               u8         total bytes allocated
;; *               u8         total instances allocated
(def-struct-tokenizer
  :HPROF_HEAP_SUMMARY ;; heap summary
  [id-type remaining-size]
  [:total-live-bytes ::tokenizer/uint32
   :total-live-instances ::tokenizer/uint32
   :total-bytes-allocated ::tokenizer/uint64
   :total-instances-allocated ::tokenizer/uint64])


(def-struct-tokenizer
  :HPROF_GC_ROOT_UNKNOWN
  [id-type]
  [:object-id id-type])

(def-struct-tokenizer
  :HPROF_GC_ROOT_THREAD_OBJ
  [id-type]
  [:thread-object-id id-type
   :thread-sequence-number ::tokenizer/uint32
   :stacktrace-sequence-number ::tokenizer/uint32])



(def-struct-tokenizer
  :HPROF_GC_ROOT_JNI_GLOBAL ;; JNI global ref root
  [id-type]
  [:object-id id-type
   :jni-global-ref-id id-type])


(def-struct-tokenizer
  :HPROF_GC_ROOT_JNI_LOCAL ;; JNI local ref
  [id-type]
  [:object-id id-type
   :thread-serial-number ::tokenizer/uint32
   :frame-number ::tokenizer/uint32])

(def-struct-tokenizer
  :HPROF_GC_ROOT_JAVA_FRAME ;; Java stack frame
  [id-type]
  [:object-id id-type
   :thread-serial-number ::tokenizer/uint32
   :frame-number ::tokenizer/uint32])

(def-struct-tokenizer
  :HPROF_GC_ROOT_NATIVE_STACK
  [id-type]
  [:object-id id-type
   :thread-serial-number ::tokenizer/uint32])

(def-struct-tokenizer
  :HPROF_GC_ROOT_STICKY_CLASS ;; System class
  [id-type]
  [:object-id id-type])

(def-struct-tokenizer
  :HPROF_GC_ROOT_THREAD_BLOCK ;; Reference from thread block
  [id-type]
  [:object-id id-type
   :thread-serial-number ::tokenizer/uint32])

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
   :stacktrace-serial-number ::tokenizer/uint32
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
   :instance-bytes-size ::tokenizer/uint32

   ;; u2         size of constant pool
   constant-pool-size ::tokenizer/uint16
   ;; AFAIK, constant-pool-size is always 0
   :constant-pool [::tokenizer/n constant-pool-size ::constant-pool-item]
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


   static-field-count ::tokenizer/uint16
   :static-fields [::tokenizer/n static-field-count [::static-field id-type]]
   ;; u2         number of static fields
   ;; [id,       static field name,
   ;;  ty,       type,
   ;;  vl]*      and value

   ;; u2         number of inst. fields (not inc. super)
   instance-field-count ::tokenizer/uint16
   ;; [id,       instance field name,
   ;;  ty]*      type
   :instance-fields [::tokenizer/n instance-field-count [::instance-field id-type]]])


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
   :stacktrace-serial-number ::tokenizer/uint32
   :class-object-id id-type
   instance-size ::tokenizer/uint32
   :instance-values [::tokenizer/bytes instance-size]])

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
   :stacktrace-serial-number ::tokenizer/uint32
   num-elements ::tokenizer/uint32
   :array-class-id id-type
   :objects [::tokenizer/n num-elements id-type]])

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
    (tokenize! ::tokenizer/n num-elements [type-name id-type])))


(def-struct-tokenizer
  :HPROF_GC_PRIM_ARRAY_DUMP ;; dump of a primitive array
  [id-type]
  [:object-id id-type
   :stacktrace-serial-number ::tokenizer/uint32
   num-elements ::tokenizer/uint32
   element-type ::tokenizer/uint8
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
  (read! ::tokenizer/reset-read-count)
  (write! [::tokenizer/start :HPROF_HEAP_DUMP_SEGMENT])
  (write! :records)
  (write! ::tokenizer/start-vec)
  (loop [remaining-bytes remaining-bytes]
    (cond
      (pos? remaining-bytes)
      (do
        #_(tokenize! ::heap-dump-record id-type)
        (tokenize! ::heap-dump-record id-type)
        (recur (- remaining-bytes
                  (read! ::tokenizer/reset-read-count))))

      (neg? remaining-bytes)
      (throw (ex-info "Offset mismatch when reading dump segment"
                      {:remaining-bytes remaining-bytes}))

      :else
      (do
        (write! ::tokenizer/end) ;; end vec
        (write! [::tokenizer/end :HPROF_HEAP_DUMP_SEGMENT])))))




(def-struct-tokenizer
  ::heap-dump-record
  [id-type]
  [subrecord-type ::tokenizer/uint8
   :heap-dump-body [::heap-dump-body id-type subrecord-type]])

(def-struct-tokenizer
  :HPROF_HEAP_DUMP_END ;; a Java stack trace
  [id-type remaining-size]
  [])


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
  (let [i (read! ::tokenizer/uint8)]
    (write! (zero? i))))

(deftokenizer :HPROF_CHAR [id-type]
  (let [c (read! ::tokenizer/uint16)]
    (write! (char c))))



(deftokenizer :HPROF_FLOAT [id-type]
  (tokenize! ::tokenizer/float32))

(deftokenizer :HPROF_DOUBLE [id-type]
  (tokenize! ::tokenizer/float64))

(deftokenizer :HPROF_BYTE [id-type]
  (tokenize! ::tokenizer/int8))

(deftokenizer :HPROF_SHORT [id-type]
  (tokenize! ::tokenizer/int16))

(deftokenizer :HPROF_INT [id-type]
  (tokenize! ::tokenizer/int32))

(deftokenizer :HPROF_LONG [id-type]
  (tokenize! ::tokenizer/int64))

(def-struct-tokenizer
  ::static-field
  [id-type]
  [:static-field-name id-type
   static-field-type ::tokenizer/uint8
   :static-field-value [::static-field-value id-type static-field-type]])

(def-struct-tokenizer
  ::instance-field
  [id-type]
  [:instance-field-name id-type
   :instance-field-type ::tokenizer/uint8])

(deftokenizer ::heap-dump-body [id-type subrecord-type]
  (let [type-name (get heap-dump-tag-name subrecord-type)]
    (assert type-name (str "Unknown heap dump subrecord type: " subrecord-type))
    (tokenize! type-name id-type)))


(deftokenizer ::tag-string [id-type remaining-bytes]
  (let [buf (read! ::tokenizer/bytes (- remaining-bytes
                                        (case id-type
                                          ::tokenizer/int32 4
                                          ::tokenizer/int64 8)))]
    (write! (String. buf "utf-8"))))


(defn tokenize-hprof [fname]
  (let [tokenizer (tokenizer/make-tokenizer tokenizer/inputstream-readers
                                            @tokenizer/tokenizers)]
    (with-open [is (io/input-stream fname)
                pbis (PushbackInputStream. is 1)]
      (persistent!
       (tokenizer identity conj! (transient []) pbis ::hprof)))))

(defn tokenize-hprof
  ([fname]
   (persistent!
    (tokenize-hprof identity conj! (transient []) fname)))
  ([xform rf init fname]
   (let [tokenizer (tokenizer/make-tokenizer tokenizer/inputstream-readers
                                             @tokenizer/tokenizers)]
     (with-open [is (io/input-stream fname)
                 pbis (PushbackInputStream. is 1)]
       (tokenizer xform rf init pbis ::hprof)))))



(defn parse-instance-values [{:keys [id->str]}
                             classes
                             instance-values]
  (let [{:keys [instance-fields]} class
        id-type ::tokenizer/int64
        fields (->> (into []
                          (comp (mapcat :instance-fields)
                                (map
                                 (fn [{:keys [instance-field-name instance-field-type]}]
                                   [(keyword (id->str instance-field-name))
                                    [(get field-name instance-field-type) id-type]])))
                          classes))
        format [::tokenizer/struct fields]]
    (let [tokenizer (tokenizer/make-tokenizer tokenizer/inputstream-readers
                                              @tokenizer/tokenizers)]
      (with-open [is (ByteArrayInputStream. instance-values)
                  pbis (PushbackInputStream. is 1)]
        (let [result (tokenizer identity tokenizer/hydrate (tokenizer/hydrate) pbis format)

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
           (if (= [::tokenizer/end type] input)
             (do (vreset! ready? false)
                 (rf result ::tokenizer/end))
             (rf result input))
           (if (= input [::tokenizer/start type])
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
  (let [parsed (reduce tokenizer/hydrate (tokenizer/hydrate) (tokenize-hprof fname))
        classes (class-defs parsed)
        id->str (->string-map parsed)
        
        class->str (class-names parsed id->str)
        instances (instance-defs parsed)
        info {:classes classes
              :id->str id->str
              :class->str class->str}

        instances (update-vals instances
                               #(hydrate-instance info %))]
    (assoc info
           :instances instances)))
(comment


  (def info (hprof-info "mem.hprof"))

  (defn rand-instance []
    (->> info
         :instances
         vals
         (filter #(-> %
                      :class-object-id
                      (->> (get (:classes info)))
                      :instance-fields
                      seq))
         (rand-nth)))
  ,)
