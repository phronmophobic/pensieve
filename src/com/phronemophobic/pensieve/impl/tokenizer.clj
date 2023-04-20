(ns com.phronemophobic.pensieve.impl.tokenizer
  (:require [com.phronemophobic.pensieve.impl.collect :as collect]
            [clojure.java.io :as io])
  (:import java.io.ByteArrayInputStream
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
           ::start (-> z
                      (collect/begin-map)
                      (hydrate :type)
                      (hydrate type))
           ::end (collect/end z)))

       :else
       (collect/append z token)))))


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
         (~write!## [::start ~name])
         (let ~bindings
           (~write!## [::end ~name]))))))

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



(deftokenizer ::n [n type]
  (write! ::start-vec)
  (loop [n n]
    (if (pos? n)
      (let [done? (tokenize! type)]
        (when (not done?)
          (recur (dec n))))
      (write! ::end))))

(deftokenizer ::+ [type]
  (write! ::start-vec)
  (loop []
    (let [eof? (read! ::eof?)]
      (if eof?
        (write! ::end)
        (do
          (tokenize! type)
          (recur))))))

(def read-sizes
  {::float32 4
   ::float64 8
   ::int16 2
   ::int32 4
   ::int64 8
   ::int8 1
   ::uint16 2
   ::uint32 4
   ::uint64 8
   ::uint8 1})

(defn wrap-read-count [read!]
  (let [read-count (volatile! 0)]
    (fn [type & args]
      (case type
        (::float32
         ::float64
         ::int16
         ::int32
         ::int64
         ::int8
         ::uint16
         ::uint32
         ::uint64
         ::uint8)
        (do
          (vswap! read-count + (read-sizes type))
          (apply read! type args))

        ::bytes
        (do
          (vswap! read-count + (first args))
          (apply read! type args))

        ::c-string
        (let [ret (read! type)]
          (vswap! read-count + (count ret))
          ret)

        ::eof? (read! type)
        ::read-count @read-count
        ::reset-read-count (let [old @read-count]
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
                    ;; unless every tokenizer is checking for reduced?
                    ;; need to always call rf.
                    ;; todo: make all the tokenizers check for reduced?
                    (vswap! ret rf x)
                    false)
           tokenize! (fn tokenize! [type & args]
                       (if (vector? type)
                         (apply tokenize! type)
                         (if-let [f (get tokenizers type)]
                           (let [f (f tokenize! read! write!)]
                             (apply f args))
                           (write! (apply read! type args)))))]
       (apply tokenize! type args)
       (rf @ret)))))
