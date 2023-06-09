(ns com.phronemophobic.pensieve.impl.collect)

(defn append [[rf result p :as z] x]
  [rf
   (rf result x)
   p])

(defn end [[rf result [prf presult pp :as p]]]
  (let [x (rf result)]
    (if p
      [prf
       (prf presult x)
       pp]
      x)))

(defn coll-rf [init]
  (fn
    ([] (transient init))
    ([result] (persistent! result))
    ([result x]
     (conj! result x))))

(defn begin-map
  ([] (begin-map nil))
  ([z]
   (let [xf (partition-all 2)
         rf (xf (coll-rf {}))]
     [rf
      (rf)
      z])))

(defn begin-vec
  ([] (begin-vec nil))
  ([z]
   (let [rf (coll-rf [])]
     [rf
      (rf)
      z])))

(defn hydrate
  ([] (hydrate nil ::start-vec))
  ([z] (hydrate z ::end))
  ([z token]
   (case token
     ::start-vec (begin-vec z)
     ::start-map (begin-map z)
     ::end (end z)
     ;; else
     (append z token))))


(comment
  (-> nil
      (hydrate ::start-map)
      (hydrate :a)
      (hydrate 42)
      (hydrate "vector")
      (hydrate ::start-vec)
      (hydrate ::end)
      (hydrate ::end))

    (-> nil
        (hydrate ::start-vec)

        (hydrate ::end))
  ,)
