(ns ^:no-doc datahike.db
  (:require
   #?(:cljs [goog.array :as garray])
   [clojure.walk]
   [clojure.data]
   [clojure.core.async :as async]
   [hitchhiker.tree.utils.cljs.async :as ha]
   ;[superv.async :refer [<?? S]]
   #?(:clj [clojure.pprint :as pp])
   [datahike.index :refer [-slice -seq -count -all -persistent! -transient] :as di]
   [datahike.datom :as dd :refer [datom datom-tx datom-added datom?]]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.tools :refer [get-time case-tree raise]]
   [datahike.schema :as ds]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [datahike.config :as dc]
   #?(:cljs [datahike.datom :refer [Datom]]))
  #?(:cljs (:require-macros [datahike.db :refer [defrecord-updatable if-cljs cond+]]
                            [datahike.datom :refer [combine-cmp]]
                            [datahike.tools :refer [case-tree raise]]))
  (:refer-clojure :exclude [seqable?])
  #?(:clj (:import [clojure.lang AMapEntry]
                   [java.util Date]
                   [datahike.datom Datom])))

;; ----------------------------------------------------------------------------

#?(:cljs
   (do
     (def Exception js/Error)
     (def IllegalArgumentException js/Error)
     (def UnsupportedOperationException js/Error)))

(def ^:const implicit-schema {:db/ident {:db/unique :db.unique/identity}
                              :db.entity/attrs {:db/cardinality :db.cardinality/many}
                              :db.entity/preds {:db/cardinality :db.cardinality/many}})

;; ----------------------------------------------------------------------------

(defn #?@(:clj  [^Boolean seqable?]
          :cljs [^boolean seqable?])
  [x]
  (and (not (string? x))
       #?(:cljs (or (cljs.core/seqable? x)
                    (arrays/array? x))
          :clj  (or (seq? x)
                    (instance? clojure.lang.Seqable x)
                    (nil? x)
                    (instance? Iterable x)
                    (arrays/array? x)
                    (instance? java.util.Map x)))))

;; ----------------------------------------------------------------------------
;; macros and funcs to support writing defrecords and updating
;; (replacing) builtins, i.e., Object/hashCode, IHashEq hasheq, etc.
;; code taken from prismatic:
;;  https://github.com/Prismatic/schema/commit/e31c419c56555c83ef9ee834801e13ef3c112597
;;

(defn- cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

#?(:clj
   (defn- get-sig [method]
     ;; expects something like '(method-symbol [arg arg arg] ...)
     ;; if the thing matches, returns [fully-qualified-symbol arity], otherwise nil
     (and (sequential? method)
          (symbol? (first method))
          (vector? (second method))
          (let [sym (first method)
                ns (or (some->> sym resolve meta :ns str) "clojure.core")]
            [(symbol ns (name sym)) (-> method second count)]))))

#?(:clj
   (defn- dedupe-interfaces [deftype-form]
     ;; get the interfaces list, remove any duplicates, similar to remove-nil-implements in potemkin
     ;; verified w/ deftype impl in compiler:
     ;; (deftype* tagname classname [fields] :implements [interfaces] :tag tagname methods*)
     (let [[deftype* tagname classname fields implements interfaces & rest] deftype-form]
       (when (or (not= deftype* 'deftype*) (not= implements :implements))
         (throw (IllegalArgumentException. "deftype-form mismatch")))
       (list* deftype* tagname classname fields implements (vec (distinct interfaces)) rest))))

#?(:clj
   (defn- make-record-updatable-clj [name fields & impls]
     (let [impl-map (->> impls (map (juxt get-sig identity)) (filter first) (into {}))
           body (macroexpand-1 (list* 'defrecord name fields impls))]
       (clojure.walk/postwalk
        (fn [form]
          (if (and (sequential? form) (= 'deftype* (first form)))
            (->> form
                 dedupe-interfaces
                 (remove (fn [method]
                           (when-let [impl (-> method get-sig impl-map)]
                             (not= method impl)))))
            form))
        body))))

#?(:clj
   (defn- make-record-updatable-cljs [name fields & impls]
     `(do
        (defrecord ~name ~fields)
        (extend-type ~name ~@impls))))

#?(:clj
   (defmacro defrecord-updatable [name fields & impls]
     `(if-cljs
       ~(apply make-record-updatable-cljs name fields impls)
       ~(apply make-record-updatable-clj name fields impls))))

;; ----------------------------------------------------------------------------

;;;;;;;;;; Searching

(defprotocol ISearch
  (-search [data pattern]))

(defprotocol IIndexAccess
  (-datoms [db index components])
  (-seek-datoms [db index components])
  (-rseek-datoms [db index components])
  (-index-range [db attr start end]))

(defprotocol IDB
  (-schema [db])
  (-rschema [db])
  (-attrs-by [db property])
  (-max-tx [db])
  (-max-eid [db])
  (-temporal-index? [db]) ;;deprecated
  (-keep-history? [db])
  (-config [db]))

(defprotocol IHistory
  (-time-point [db])
  (-origin [db]))

;; ----------------------------------------------------------------------------


(declare hash-datoms equiv-db empty-db resolve-datom validate-attr components->pattern indexing?)
#?(:cljs (declare pr-db))

(defn db-transient [db]
  (-> db
      (update :eavt -transient)
      (update :aevt -transient)
      (update :avet -transient)))

(defn db-persistent! [db]
  (-> db
      (update :eavt -persistent!)
      (update :aevt -persistent!)
      (update :avet -persistent!)))

(defn- search-indices [eavt aevt avet pattern indexed? temporal-db?]
  (let [[e a v tx added?] pattern
        ;; TODO: make this better with alts higher up call stack
        empty-list-chan (async/go (async/>! (async/chan) '()))]
    (if (and (not temporal-db?) (false? added?))
      empty-list-chan
      (ha/go-try
       (case-tree [e a (some? v) tx]
                  [(ha/<? (-slice eavt (datom e a v tx) (datom e a v tx) :eavt)) ;; e a v tx
                   (ha/<? (-slice eavt (datom e a v tx0) (datom e a v txmax) :eavt)) ;; e a v _
                   (->> (ha/<? (-slice eavt (datom e a nil tx0) (datom e a nil txmax) :eavt)) ;; e a _ tx
                        (filter (fn [d] (= tx (datom-tx d)))))
                   (ha/<? (-slice eavt (datom e a nil tx0) (datom e a nil txmax) :eavt)) ;; e a _ _
                   (->> (ha/<? (-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt)) ;; e _ v tx
                        (filter (fn [^Datom d] (and (= v (.-v d))
                                                    (= tx (datom-tx d))))))
                   (->> (ha/<? (-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt)) ;; e _ v _
                        (filter (fn [^Datom d] (= v (.-v d)))))
                   (->> (ha/<? (-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt)) ;; e _ _ tx
                        (filter (fn [^Datom d] (= tx (datom-tx d)))))
                   (ha/<? (-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt)) ;; e _ _ _
                   (if indexed?                              ;; _ a v tx
                     (->> (ha/<? (-slice avet (datom e0 a v tx0) (datom emax a v txmax) :avet))
                          (filter (fn [^Datom d] (= tx (datom-tx d)))))
                     (->> (ha/<? (-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt))
                          (filter (fn [^Datom d] (and (= v (.-v d))
                                                      (= tx (datom-tx d)))))))
                   (if indexed?                              ;; _ a v _
                     (ha/<? (-slice avet (datom e0 a v tx0) (datom emax a v txmax) :avet))
                     (->> (ha/<? (-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt))
                          (filter (fn [^Datom d] (= v (.-v d))))))
                   (->> (ha/<? (-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)) ;; _ a _ tx
                        (filter (fn [d] (= tx (datom-tx d)))))
                   (ha/<? (-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)) ;; _ a _ _
                   (filter (fn [^Datom d] (and (= v (.-v d)) (= tx (datom-tx d)))) (ha/<? (-all eavt))) ;; _ _ v tx
                   (filter (fn [^Datom d] (= v (.-v d))) (ha/<? (-all eavt))) ;; _ _ v _
                   (filter (fn [d] (= tx (datom-tx d))) (ha/<? (-all eavt))) ;; _ _ _ tx
                   (ha/<? (-all eavt))])))))

(defn diff-similar
  "Utility function like clojure.data/diff-similar but might return a core.async channel depending on *if-async*"
  [a b]
  (ha/go-try
   (let [datoms-a (ha/<? (-slice (:eavt a) (datom e0 nil nil tx0) (datom emax nil nil txmax) :eavt))
         datoms-b (ha/<? (-slice (:eavt b) (datom e0 nil nil tx0) (datom emax nil nil txmax) :eavt))]
     (dd/diff-sorted datoms-a datoms-b dd/cmp-datoms-eavt-quick))))

(defn count<
  "Utility function for counting all datoms."
  [db]
  (di/-count (:eavt db)))

(defn seq<
  "Utility function for returning a seq of datoms in eavt order."
  [db]
  (-seq (:eavt db)))


(defrecord-updatable DB [schema eavt aevt avet temporal-eavt temporal-aevt temporal-avet max-eid max-tx rschema hash config]
  #?@(:cljs
      [IHash (-hash [db] hash)
       IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (throw (ex-info "seq not supported for async implementation. Use seq< utility function." {})))
       IReversible (-rseq [db] (throw (ex-info "rseq not supported for async implementation." {})))
       ;; could fix with keeping active count of database
       ICounted (-count [db] (throw (ex-info "count not supported for async implementation. Use count< utility function." {})))
       IEmptyableCollection (-empty [db] (throw (ex-info "empty not supported for async implementation. Use empty-db directly." {})))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))
       IEditableCollection (-as-transient [db] (db-transient db))
       ITransientCollection (-conj! [db key] (throw (ex-info "datahike.DB/conj! is not supported" {})))
       (-persistent! [db] (db-persistent! db))]

      :clj
      [Object (hashCode [db] hash)
       clojure.lang.IHashEq (hasheq [db] hash)
       clojure.lang.Seqable (seq [db] (-seq eavt))
       clojure.lang.IPersistentCollection
       (count [db] (-count eavt))
       (equiv [db other] (ha/<?? (equiv-db db other)))
       (empty [db] (empty-db schema))
       clojure.lang.IEditableCollection
       (asTransient [db] (db-transient db))
       clojure.lang.ITransientCollection
       (conj [db key] (throw (ex-info "datahike.DB/conj! is not supported" {})))
       (persistent [db] (db-persistent! db))])

  IDB
  (-schema [db] (.-schema db))
  (-rschema [db] (.-rschema db))
  (-attrs-by [db property] ((.-rschema db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-> db -config :keep-history?))
  (-max-tx [db] (.-max-tx db))
  (-max-eid [db] (.-max-eid db))
  (-config [db] (.-config db))

  ISearch
  (-search [db pattern]
           (ha/go-try
            (let [[_ a _ _] pattern
                  {:keys [eavt aevt avet]} db]
              (ha/<? (search-indices eavt aevt avet pattern (indexing? db a) false)))))

  IIndexAccess
  (-datoms [db index-type cs]
           (ha/go-try
            (ha/<?
             (-slice (get db index-type)
                     (ha/<? (components->pattern db index-type cs e0 tx0))
                     (ha/<? (components->pattern db index-type cs emax txmax))
                     index-type))))

  (-seek-datoms [db index-type cs]
                (ha/go-try
                 (ha/<? (-slice (get db index-type)
                                (ha/<? (components->pattern db index-type cs e0 tx0))
                                (datom emax nil nil txmax)
                                index-type))))

  (-rseek-datoms [db index-type cs]
                 (ha/go-try
                  (rseq (vec (ha/<? (-slice (get db index-type)
                                            (ha/<? (components->pattern db index-type cs e0 tx0))
                                            (datom emax nil nil txmax)
                                            index-type))))))

  (-index-range [db attr start end]
                (when-not (indexing? db attr)
                  (raise "Attribute" attr "should be marked as :db/index true" {}))
                (validate-attr attr (list '-index-range 'db attr start end) db)
                (ha/go-try
                 (let [{:keys [avet]} db]
                   (ha/<? (-slice avet
                                  (ha/<? (resolve-datom db nil attr start nil e0 tx0))
                                  (ha/<? (resolve-datom db nil attr end nil emax txmax))
                                  :avet)))))

  clojure.data/EqualityPartition
  (equality-partition [x] :datahike/db)



  #?@(:clj
      [clojure.data/Diff
       (diff-similar [a b]
                     (let [datoms-a (ha/<?? (-slice (:eavt a) (datom e0 nil nil tx0) (datom emax nil nil txmax) :eavt))
                           datoms-b (ha/<?? (-slice (:eavt b) (datom e0 nil nil tx0) (datom emax nil nil txmax) :eavt))]
                       (dd/diff-sorted datoms-a datoms-b dd/cmp-datoms-eavt-quick)))]))


(defn db? [x]
  (and (satisfies? ISearch x)
       (satisfies? IIndexAccess x)
       (satisfies? IDB x)))

;; ----------------------------------------------------------------------------
(defrecord-updatable FilteredDB [unfiltered-db pred]
  #?@(:cljs
      [;IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (throw (ex-info "seq not supported for async implementation. Use seq< utility function." {})))
              ;; could fix with keeping active count of database
       ICounted (-count [db] (throw (ex-info "count not supported for async implementation. Use count< utility function." {})))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on FilteredDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on FilteredDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on FilteredDB"))))

       IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on FilteredDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on FilteredDB")))]

      :clj
      [clojure.lang.IPersistentCollection
       (count [db] (count (ha/<?? (-datoms db :eavt []))))
       (equiv [db o] (ha/<?? (equiv-db db o)))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on FilteredDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on FilteredDB")))

       clojure.lang.Seqable (seq [db] (ha/<?? (-datoms db :eavt [])))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on FilteredDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on FilteredDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on FilteredDB")))

       clojure.lang.Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on FilteredDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on FilteredDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on FilteredDB")))])

  IDB
  (-schema [db] (-schema (.-unfiltered-db db)))
  (-rschema [db] (-rschema (.-unfiltered-db db)))
  (-attrs-by [db property] (-attrs-by (.-unfiltered-db db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-keep-history? (.-unfiltered-db db)))
  (-max-tx [db] (-max-tx (.-unfiltered-db db)))
  (-max-eid [db] (-max-eid (.-unfiltered-db db)))
  (-config [db] (-config (.-unfiltered-db db)))

  ISearch
  (-search [db pattern]
           (ha/go-try
            (ha/<? (ha/filter< (.-pred db) (ha/<? (-search (.-unfiltered-db db) pattern))))))

  IIndexAccess
  (-datoms [db index cs]
           (ha/go-try
            (ha/<? (ha/filter< (.-pred db) (ha/<? (-datoms (.-unfiltered-db db) index cs))))))


  (-seek-datoms [db index cs]
                (ha/go-try
                 (do
                   (println "-seek-datoms" (.-pred db))
                   (filter (.-pred db) (ha/<? (-seek-datoms (.-unfiltered-db db) index cs))))))

  (-rseek-datoms [db index cs]
                 (ha/go-try
                  (do
                    (println (.-pred db))
                    (filter (.-pred db) (ha/<? (-rseek-datoms (.-unfiltered-db db) index cs))))))

  (-index-range [db attr start end]
                (ha/go-try
                 (do
                   (println "-index-range " (.-pred db)))
                 (filter (.-pred db) (ha/<? (-index-range (.-unfiltered-db db) attr start end))))))

(defn- search-current-indices [^DB db pattern]
  (let [[_ a _ _] pattern]
    (search-indices (.-eavt db)
                    (.-aevt db)
                    (.-avet db)
                    pattern
                    (indexing? db a)
                    false)))

(defn- search-temporal-indices [^DB db pattern]
  (ha/go-try
   (let [[_ a _ _ added] pattern
         result (ha/<? (search-indices (.-temporal-eavt db)
                                       (.-temporal-aevt db)
                                       (.-temporal-avet db)
                                       pattern
                                       (indexing? db a)
                                       true))]
     (case added
       true (filter datom-added result)
       false (remove datom-added result)
       nil result))))

(defn temporal-search [^DB db pattern]
  (ha/go-try
   (concat (ha/<? (search-current-indices db pattern))
           (ha/<? (search-temporal-indices db pattern)))))

(defn temporal-datoms [^DB db index-type cs]
  (ha/go-try
   (let [index (get db index-type)
         temporal-index (get db (keyword (str "temporal-" (name index-type))))
         from (ha/<? (components->pattern db index-type cs e0 tx0))
         to (ha/<? (components->pattern db index-type cs emax txmax))]
     (concat (ha/<? (-slice index from to index-type))
             (ha/<? (-slice temporal-index from to index-type))))))

(defn temporal-seek-datoms [^DB db index-type cs]
  (ha/go-try
   (let [index (get db index-type)
         temporal-index (get db (keyword (str "temporal-" (name index-type))))
         from (ha/<? (components->pattern db index-type cs e0 tx0))
         to (datom emax nil nil txmax)]
     (concat  (ha/<? (-slice index from to index-type))
              (ha/<? (-slice temporal-index from to index-type))))))

(defn temporal-rseek-datoms [^DB db index-type cs]
  (ha/go-try
   (let [index (get db index-type)
         temporal-index (get db (keyword (str "temporal-" (name index-type))))
         from (ha/<? (components->pattern db index-type cs e0 tx0))
         to (datom emax nil nil txmax)]
     (concat ;; check what this concat does
      (-> (concat  (ha/<? (-slice index from to index-type))
                   (ha/<? (-slice temporal-index from to index-type)))
          vec
          rseq)))))

(defn temporal-index-range [^DB db current-db attr start end]
  (when-not (indexing? db attr)
    (raise "Attribute" attr "should be marked as :db/index true" {}))
  (validate-attr attr (list '-index-range 'db attr start end) db)
  (ha/go-try
   (let [from (ha/<? (resolve-datom current-db nil attr start nil e0 tx0))
         to (ha/<? (resolve-datom current-db nil attr end nil emax txmax))]
     (concat
      (ha/<? (-slice (get db :avet) from to :avet))
      (ha/<? (-slice (get db :temporal-avet) from to :avet))))))

(defrecord-updatable HistoricalDB [origin-db]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (throw (ex-info "seq not supported for async implementation. Use seq< utility function." {})))
       ;; could fix with keeping active count of database
       ICounted (-count [db] (throw (ex-info "count not supported for async implementation. Use count< utility function." {})))
       
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))
       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on HistoricalDB")))
       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on HistoricalDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on HistoricalDB"))))

       IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on HistoricalDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on HistoricalDB")))]
      :clj
      [clojure.lang.IPersistentCollection
       (count [db] (count (ha/<?? (-datoms db :eavt []))))
       (equiv [db o] (ha/<?? (equiv-db db o)))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on HistoricalDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on HistoricalDB")))

       clojure.lang.Seqable (seq [db] (ha/<?? (-datoms db :eavt [])))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on HistoricalDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on HistoricalDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on HistoricalDB")))

       clojure.lang.Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on HistoricalDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on HistoricalDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on HistoricalDB")))])

  IDB
  (-schema [db] (-schema (.-origin-db db)))
  (-rschema [db] (-rschema (.-origin-db db)))
  (-attrs-by [db property] (-attrs-by (.-origin-db db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-keep-history? (.-origin-db db)))
  (-max-tx [db] (-max-tx (.-origin-db db)))
  (-max-eid [db] (-max-eid (.-origin-db db)))
  (-config [db] (-config (.-origin-db db)))

  IHistory
  (-origin [db] (.-origin-db db))

  ISearch
  (-search [db pattern]
           (ha/go-try
            (ha/<? (temporal-search (.-origin-db db) pattern))))

  IIndexAccess
  (-datoms [db index-type cs] (ha/go-try (ha/<? (temporal-datoms (.-origin-db db) index-type cs))))

  (-seek-datoms [db index-type cs] (ha/go-try (ha/<? (temporal-seek-datoms (.-origin-db db) index-type cs))))

  (-rseek-datoms [db index-type cs] (ha/go-try (ha/<? (temporal-rseek-datoms (.-origin-db db) index-type cs))))

  (-index-range [db attr start end] (ha/go-try (ha/<? (temporal-index-range (.-origin-db db) db attr start end)))))

(defn filter-txInstant [datoms pred db]  ; TODO: was a transducer before. May want to keep the transducer for JVM
  (ha/go-try
   (->> datoms
        (map datom-tx)
        (distinct)
        (map (fn [tx] (ha/go-try (ha/<? (temporal-datoms db :eavt [tx])))))
        (async/merge)
        (async/into [])
        (ha/<?)
        (apply concat)
        (keep (fn [^Datom d]
                (when (and (= :db/txInstant (.-a d)) (pred d))
                  (.-e d))))

        (into #{}))))

(defn get-current-values [rschema datoms]
  (->> datoms
       (filter datom-added)
       (group-by (fn [^Datom datom] [(.-e datom) (.-a datom)]))
       (mapcat
        (fn [[[_ a] entities]]
          (if (contains? (get-in rschema [:db.cardinality/many]) a)
            entities
            [(reduce (fn [^Datom datom-0 ^Datom datom-1]
                       (if (> (datom-tx datom-0) (datom-tx datom-1))
                         datom-0
                         datom-1)) entities)])))))

(defn- date? [d]
  #?(:cljs (instance? js/Date d)
     :clj  (instance? Date d)))

(defn filter-as-of-datoms [datoms time-point db]
  (ha/go-try
   (let [as-of-pred (fn [^Datom d]
                      (if (date? time-point)
                        (< (.getTime ^Date (.-v d)) (.getTime ^Date time-point))
                        (<= (dd/datom-tx d) time-point)))
         filtered-tx-ids (ha/<? (filter-txInstant datoms as-of-pred db))
         filtered-datoms (->> datoms
                              (filter (fn [^Datom d] (contains? filtered-tx-ids (datom-tx d))))
                              (get-current-values (-rschema db)))]
     filtered-datoms)))

(defrecord-updatable AsOfDB [origin-db time-point]
  #?@(:cljs
      [IEquiv (-equiv [db other] (ha/<?? (equiv-db db other)))
       ISeqable (-seq [db] (ha/<?? (-datoms db :eavt [])))
       ICounted (-count [db] (count (ha/<?? (-datoms db :eavt []))))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on AsOfDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on AsOfDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on AsOfDB"))))

       IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on AsOfDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on AsOfDB")))]
      :clj
      [clojure.lang.IPersistentCollection
       (count [db] (count (ha/<?? (-datoms db :eavt []))))
       (equiv [db o] (ha/<?? (equiv-db db o)))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on AsOfDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on AsOfDB")))

       clojure.lang.Seqable (seq [db] (ha/<?? (-datoms db :eavt [])))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on AsOfDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on AsOfDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on AsOfDB")))

       clojure.lang.Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on AsOfDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on AsOfDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on AsOfDB")))])

  IDB
  (-schema [db] (-schema (.-origin-db db)))
  (-rschema [db] (-rschema (.-origin-db db)))
  (-attrs-by [db property] (-attrs-by (.-origin-db db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-keep-history? (.-origin-db db)))
  (-max-tx [db] (-max-tx (.-origin-db db)))
  (-max-eid [db] (-max-eid (.-origin-db db)))
  (-config [db] (-config (.-origin-db db)))

  IHistory
  (-time-point [db] (.-time-point db))
  (-origin [db] (.-origin-db db))

  ISearch
  (-search [db pattern]
           (let [origin-db (.-origin-db db)]
             (ha/go-try (-> (ha/<? (temporal-search origin-db pattern))
                            (filter-as-of-datoms (.-time-point db) origin-db)
                            (ha/<?)))))

  IIndexAccess
  (-datoms [db index-type cs]
           (let [origin-db (.-origin-db db)]
             (ha/go-try
              (-> (ha/<? (temporal-datoms origin-db index-type cs))
                  (filter-as-of-datoms (.-time-point db) origin-db)
                  (ha/<?)))))

  (-seek-datoms [db index-type cs]
                (let [origin-db (.-origin-db db)]
                  (ha/go-try
                   (-> (ha/<? (temporal-seek-datoms origin-db index-type cs))
                       (filter-as-of-datoms (.-time-point db) origin-db)
                       (ha/<?)))))

  (-rseek-datoms [db index-type cs]
                 (let [origin-db (.-origin-db db)]
                   (ha/go-try
                    (-> (ha/<? (temporal-rseek-datoms origin-db index-type cs))
                        (filter-as-of-datoms (.-time-point db) origin-db)
                        (ha/<?)))))

  (-index-range [db attr start end]
                (let [origin-db (.-origin-db db)]
                  (ha/go-try
                   (-> (ha/<? (temporal-index-range origin-db db attr start end))
                       (filter-as-of-datoms (.-time-point db) origin-db)
                       (ha/<?))))))

(defn- filter-since [datoms time-point db]
  (ha/go-try
   (let [since-pred (fn [^Datom d]
                      (if (date? time-point)
                        (> (.getTime ^Date (.-v d)) (.getTime ^Date time-point))
                        (>= (.-tx d) time-point)))
         filtered-tx-ids (ha/<? (filter-txInstant datoms since-pred db))]
     (->> datoms
          (filter datom-added)
          (filter (fn [^Datom d] (contains? filtered-tx-ids (datom-tx d))))))))

(defn- filter-before [datoms ^Date before-date db]
  (ha/go-try
   (let [before-pred (fn [^Datom d]
                       (< (.getTime ^Date (.-v d)) (.getTime before-date)))
         filtered-tx-ids (ha/<? (filter-txInstant datoms before-pred db))]
     (filter
      (fn [^Datom d]
        (contains? filtered-tx-ids (datom-tx d)))
      datoms))))

(defrecord-updatable SinceDB [origin-db time-point]
  #?@(:cljs
      [IEquiv (-equiv [db other] (ha/<?? (equiv-db db other)))
       ISeqable (-seq [db] (ha/<?? (-datoms db :eavt [])))
       ICounted (-count [db] (count (ha/<?? (-datoms db :eavt []))))
       IPrintWithWriter (-pr-writer [db w opts] (do (println "Point: 2") (pr-db db w opts)))
       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on SinceDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on SinceDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on SinceDB"))))

       IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on SinceDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on SinceDB")))]
      :clj
      [clojure.lang.IPersistentCollection
       (count [db] (count (ha/<?? (-datoms db :eavt []))))
       (equiv [db o] (ha/<?? (equiv-db db o)))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on SinceDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on SinceDB")))

       clojure.lang.Seqable (seq [db] (ha/<?? (-datoms db :eavt [])))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on SinceDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on SinceDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on SinceDB")))

       clojure.lang.Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on SinceDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on SinceDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on SinceDB")))])

  IDB
  (-schema [db] (-schema (.-origin-db db)))
  (-rschema [db] (-rschema (.-origin-db db)))
  (-attrs-by [db property] (-attrs-by (.-origin-db db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-keep-history? (.-origin-db db)))
  (-max-tx [db] (-max-tx (.-origin-db db)))
  (-max-eid [db] (-max-eid (.-origin-db db)))
  (-config [db] (-config (.-origin-db db)))

  IHistory
  (-time-point [db] (.-time-point db))
  (-origin [db] (.-origin-db db))

  ISearch
  (-search [db pattern]
           (let [origin-db (.-origin-db db)]
             (ha/go-try (-> (ha/<? (temporal-search origin-db pattern))
                            (filter-since (.-time-point db) origin-db)
                            (ha/<?)))))

  IIndexAccess
  (-datoms [db index-type cs]
           (let [origin-db (.-origin-db db)]
             (ha/go-try
              (-> (ha/<? (temporal-datoms origin-db index-type cs))
                  (filter-since (.-time-point db) origin-db)
                  (ha/<?)))))

  (-seek-datoms [db index-type cs]
                (let [origin-db (.-origin-db db)]
                  (ha/go-try
                   (-> (ha/<? (temporal-seek-datoms origin-db index-type cs))
                       (filter-since (.-time-point db) origin-db)
                       (ha/<?)))))

  (-rseek-datoms [db index-type cs]
                 (let [origin-db (.-origin-db db)]
                   (ha/go-try
                    (-> (ha/<? (temporal-rseek-datoms origin-db index-type cs))
                        (filter-since (.-time-point db) origin-db)
                        (ha/<?)))))

  (-index-range [db attr start end]
                (let [origin-db (.-origin-db db)]
                  (ha/go-try
                   (-> (ha/<? (temporal-index-range origin-db db attr start end))
                       (filter-since (.-time-point db) origin-db)
                       (ha/<?))))))

;; ----------------------------------------------------------------------------

(defn attr->properties [k v]
  (case v
    :db.unique/identity [:db/unique :db.unique/identity :db/index]
    :db.unique/value [:db/unique :db.unique/value :db/index]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref [:db.type/ref :db/index]
    (if (= k :db/ident)
      [:db/ident]
      (when (true? v)
        (case k
          :db/isComponent [:db/isComponent]
          :db/index [:db/index]
          :db/noHistory [:db/noHistory]
          [])))))

(defn- rschema [schema]
  (reduce-kv
   (fn [m attr keys->values]
     (if (keyword? keys->values)
       m
       (reduce-kv
        (fn [m key value]
          (reduce
           (fn [m prop]
             (assoc m prop (conj (get m prop #{}) attr)))
           m (attr->properties key value)))
        (update m :db/ident (fn [coll] (if coll (conj coll attr) #{attr}))) keys->values)))
   {} schema))

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v)
                (contains? expected v))
    (throw (ex-info (str "Bad attribute specification for " (pr-str {a {k v}}) ", expected one of " expected)
                    {:error     :schema/validation
                     :attribute a
                     :key       k
                     :value     v}))))

(defn- validate-schema [schema]
  (doseq [[a kv] schema]
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not= (:db/valueType kv) :db.type/ref))
        (throw (ex-info (str "Bad attribute specification for " a ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}")
                        {:error     :schema/validation
                         :attribute a
                         :key       :db/isComponent}))))
    (validate-schema-key a :db/unique (:db/unique kv) #{:db.unique/value :db.unique/identity})
    (validate-schema-key a :db/valueType (:db/valueType kv) #{:db.type/ref})
    (validate-schema-key a :db/cardinality (:db/cardinality kv) #{:db.cardinality/one :db.cardinality/many})))

(def ^:const br 300)
(def ^:const br-sqrt (long (Math/sqrt br)))

(defn to-old-schema [new-schema]
  (if (or (vector? new-schema) (seq? new-schema))
    (reduce
     (fn [acc {:keys [:db/ident] :as schema-entity}]
       (assoc acc ident schema-entity))
     {}
     new-schema)
    new-schema))

(defn- validate-write-schema [schema]
  (when-not (ds/old-schema-valid? schema)
    (raise "Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
           (ds/explain-old-schema schema))))

(defn ^DB empty-db
  "Prefer create-database in api, schema not in index."
  ([] (empty-db nil nil))
  ([schema] (empty-db schema nil))
  ([schema config]
   {:pre [(or (nil? schema) (map? schema) (coll? schema))]}
   (let [{:keys [keep-history? index schema-flexibility] :as config} (merge (dc/storeless-config) config)
         on-read? (= :read schema-flexibility)
         schema (to-old-schema schema)
         _ (if on-read?
             (validate-schema schema)
             (validate-write-schema schema))]
     (ha/go-try (map->DB
                 (merge
                  {:schema  (merge implicit-schema schema)
                   :rschema (rschema (merge implicit-schema schema))
                   :config  config
                   :eavt    (ha/<? (di/empty-index index :eavt))
                   :aevt    (ha/<? (di/empty-index index :aevt))
                   :avet    (ha/<? (di/empty-index index :avet))
                   :max-eid e0
                   :max-tx  tx0
                   :hash    0}
                  (when keep-history?
                    {:temporal-eavt (ha/<? (di/empty-index index :eavt))
                     :temporal-aevt (ha/<? (di/empty-index index :aevt))
                     :temporal-avet (ha/<? (di/empty-index index :avet))})))))))

(defn init-max-eid [eavt]
  ;; solved with reserse slice first in datascript
  (ha/go-try
   (if-let [datoms (ha/<? (-slice
                           eavt
                           (datom e0 nil nil tx0)
                           (datom (dec tx0) nil nil txmax)
                           :eavt))]
     (-> datoms vec rseq first :e)                           ;; :e of last datom in slice
     e0)))

(defn get-max-tx [eavt]
  (ha/go-try
   (transduce (map (fn [^Datom d] (datom-tx d))) max tx0 (ha/<? (-all eavt)))))

(defn ^DB init-db
  ([datoms] (init-db datoms nil nil))
  ([datoms schema] (init-db datoms schema nil))
  ([datoms schema config]
   (validate-schema schema)
   (ha/go-try (let [{:keys [index schema-flexibility keep-history?] :as config} (merge (dc/storeless-config) config)
                    rschema (rschema (merge implicit-schema schema))
                    indexed (:db/index rschema)
                    eavt (ha/<? (di/init-index index datoms indexed :eavt))
                    aevt (ha/<? (di/init-index index datoms indexed :aevt))
                    avet (ha/<? (di/init-index index datoms indexed :avet))
                    max-eid (ha/<? (init-max-eid eavt))
                    max-tx (ha/<? (get-max-tx eavt))]
                (map->DB (merge {:schema  (merge schema (when (= :read schema-flexibility) implicit-schema))
                                 :rschema rschema
                                 :config  config
                                 :eavt    eavt
                                 :aevt    aevt
                                 :avet    avet
                                 :max-eid max-eid
                                 :max-tx  max-tx
                                 :hash    (hash-datoms datoms)}
                                (when keep-history?
                                  {:temporal-eavt (ha/<? (di/empty-index index :eavt))
                                   :temporal-aevt (ha/<? (di/empty-index index :aevt))
                                   :temporal-avet (ha/<? (di/empty-index index :avet))})))))))

(defn- equiv-db-index [x y]
  (loop [xs (seq x)
         ys (seq y)]
    (cond
      (nil? xs) (nil? ys)
      (= (first xs) (first ys)) (recur (next xs) (next ys))
      :else false)))

(defn- hash-datoms
  [datoms]
  (reduce #(+ %1 (hash %2)) 0 datoms))


(defn- equiv-db [db other]
  ;(ha/go-try)
  (if
   (and (instance? DB db) (instance? DB other)) (and (= (-schema db) (-schema other))
                                                     (= (:hash db) (:hash other)))
   false
   #_(equiv-db-index (ha/<? (-datoms db :eavt [])) (ha/<? (-datoms other :eavt [])))))

(defn equiv-db<
  ([db other]
   (ha/go-try
    (if
     (and (instance? DB db) (instance? DB other)) (and (= (-schema db) (-schema other))
                                                       (= (:hash db) (:hash other)))
     (and (= (-schema db) (-schema other))
          (equiv-db-index (ha/<? (-datoms db :eavt [])) (ha/<? (-datoms other :eavt [])))))))
  ([db other & more]
   (ha/go-try
    (loop [=? (ha/<? (equiv-db< db other))
           more more]
      (if-not =?
        false
        (if (seq more)
          (recur (ha/<? (equiv-db< db (first more)))
                 (rest more))
          true))))))


#?(:cljs
   (defn pr-db [db w opts]
     (-write w "#datahike/DB {")
     (-write w (str ":max-tx " (-max-tx db) " "))
     (-write w (str ":max-eid " (-max-eid db) " "))
     (-write w "}")))

#?(:clj
   (do
     (defn pr-db [db, ^java.io.Writer w]
       (.write w (str "#datahike/DB {"))
       (.write w (str ":max-tx " (-max-tx db) " "))
       (.write w (str ":max-eid " (-max-eid db)))
       (.write w "}"))

     (defn pr-hist-db [db ^java.io.Writer w flavor time-point?]
       (.write w (str "#datahike/" flavor " {"))
       (.write w ":origin ")
       (binding [*out* w]
         (pr (-origin db)))
       (when time-point?
         (.write w " :time-point ")
         (binding [*out* w]
           (pr (-time-point db))))
       (.write w "}"))

     (defmethod print-method DB [db w] (pr-db db w))
     (defmethod print-method FilteredDB [db w] (pr-db db w))
     (defmethod print-method HistoricalDB [db w] (pr-hist-db db w "HistoricalDB" false))
     (defmethod print-method AsOfDB [db w] (pr-hist-db db w "AsOfDB" true))
     (defmethod print-method SinceDB [db w] (pr-hist-db db w "SinceDB" true))

     (defmethod pp/simple-dispatch Datom [^Datom d]
       (pp/pprint-logical-block :prefix "#datahike/Datom [" :suffix "]"
                                (pp/write-out (.-e d))
                                (.write ^java.io.Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (.-a d))
                                (.write ^java.io.Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (.-v d))
                                (.write ^java.io.Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (datom-tx d))))

     (defn- pp-db [db ^java.io.Writer w]
       (pp/pprint-logical-block :prefix "#datahike/DB {" :suffix "}"
                                (pp/pprint-logical-block
                                 (pp/write-out :max-tx)
                                 (.write ^java.io.Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out (-max-tx db))
                                 (.write ^java.io.Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out :max-eid)
                                 (.write ^java.io.Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out (-max-eid db)))
                                (pp/pprint-newline :linear)))

     (defmethod pp/simple-dispatch DB [db] (pp-db db *out*))
     (defmethod pp/simple-dispatch FilteredDB [db] (pp-db db *out*))))

(defn db-from-reader [{:keys [schema datoms]}]
  (init-db (map (fn [[e a v tx]] (datom e a v tx)) datoms) schema))

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn- resolve-datom [db e a v t default-e default-tx]
  (when a (validate-attr a (list 'resolve-datom 'db e a v t) db))
  (ha/go-try
   (datom
    (or (ha/<? (entid-some db e)) default-e)                ;; e
    a                                                       ;; a
    (if (and (some? v) (ref? db a))                         ;; v
      (ha/<? (entid-strict db v))
      v)
    (or (ha/<? (entid-some db t)) default-tx))))            ;; t

(defn components->pattern [db index [c0 c1 c2 c3] default-e default-tx]
  (case index
    :eavt (resolve-datom db c0 c1 c2 c3 default-e default-tx)
    :aevt (resolve-datom db c1 c0 c2 c3 default-e default-tx)
    :avet (resolve-datom db c2 c0 c1 c3 default-e default-tx)))

;; ----------------------------------------------------------------------------

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

(defn #?@(:clj  [^Boolean is-attr?]
          :cljs [^boolean is-attr?]) [db attr property]
  (contains? (-attrs-by db property) attr))

(defn #?@(:clj  [^Boolean multival?]
          :cljs [^boolean multival?]) [db attr]
  (is-attr? db attr :db.cardinality/many))

(defn #?@(:clj  [^Boolean ref?]
          :cljs [^boolean ref?]) [db attr]
  (is-attr? db attr :db.type/ref))

(defn #?@(:clj  [^Boolean component?]
          :cljs [^boolean component?]) [db attr]
  (is-attr? db attr :db/isComponent))

(defn #?@(:clj  [^Boolean indexing?]
          :cljs [^boolean indexing?]) [db attr]
  (is-attr? db attr :db/index))

(defn #?@(:clj  [^Boolean no-history?]
          :cljs [^boolean no-history?]) [db attr]
  (is-attr? db attr :db/noHistory))

(defn entid [db eid]
  {:pre [(db? db)]}
  (ha/go-try
   (cond
     (and (number? eid) (pos? eid))
     eid

     (sequential? eid)
     (let [[attr value] eid]
       (cond
         (not= (count eid) 2)
         (raise "Lookup ref should contain 2 elements: " eid
                {:error :lookup-ref/syntax, :entity-id eid})
         (not (is-attr? db attr :db/unique))
         (raise "Lookup ref attribute should be marked as :db/unique: " eid
                {:error :lookup-ref/unique, :entity-id eid})
         (nil? value)
         nil
         :else
         (-> (ha/<? (-datoms db :avet eid)) first :e)))

     #?@(:cljs [(array? eid) (ha/<? (entid db (array-seq eid)))])  ;; TODO: reintroduce recur here?

     (keyword? eid)
     (-> (ha/<? (-datoms db :avet [:db/ident eid])) first :e)

     :else
     (raise "Expected number or lookup ref for entity id, got " eid
            {:error :entity-id/syntax, :entity-id eid}))))

(defn entid-strict [db eid]
  (ha/go-try
   (or (ha/<? (entid db eid))
       (raise "Nothing found for entity id " eid
              {:error     :entity-id/missing
               :entity-id eid}))))

(defn entid-some [db eid]
  (ha/go-try
   (when eid
     (ha/<? (entid-strict db eid)))))

;;;;;;;;;; Transacting
(defn #?@(:clj  [^Boolean reverse-ref?]
          :cljs [^boolean reverse-ref?]) [attr]
  (cond
    (keyword? attr)
    (= \_ (nth (name attr) 0))

    (string? attr)
    (boolean (re-matches #"(?:([^/]+)/)?_([^/]+)" attr))

    :else
    (raise "Bad attribute type: " attr ", expected keyword or string"
           {:error :transact/syntax, :attribute attr})))

(defn reverse-ref [attr]
  (cond
    (keyword? attr)
    (if (reverse-ref? attr)
      (keyword (namespace attr) (subs (name attr) 1))
      (keyword (namespace attr) (str "_" (name attr))))

    (string? attr)
    (let [[_ ns name] (re-matches #"(?:([^/]+)/)?([^/]+)" attr)]
      (if (= \_ (nth name 0))
        (if ns (str ns "/" (subs name 1)) (subs name 1))
        (if ns (str ns "/_" name) (str "_" name))))

    :else
    (raise "Bad attribute type: " attr ", expected keyword or string"
           {:error :transact/syntax, :attribute attr})))

(defn validate-datom [db ^Datom datom]
  (ha/go-try
   (when (and (datom-added datom)
              (is-attr? db (.-a datom) :db/unique))
     (when-let [found (not-empty (ha/<? (-datoms db :avet [(.-a datom) (.-v datom)])))]
       (raise "Cannot add " datom " because of unique constraint: " found
              {:error     :transact/unique
               :attribute (.-a datom)
               :datom     datom})))))

(defn- validate-eid [eid at]
  (when-not (number? eid)
    (raise "Bad entity id " eid " at " at ", expected number"
           {:error :transact/syntax, :entity-id eid, :context at})))

(defn- validate-attr [attr at db]
  (if (= :read (get-in db [:config :schema-flexibility]))
    (when-not (or (keyword? attr) (string? attr))
      (raise "Bad entity attribute " attr " at " at ", expected keyword or string"
             {:error :transact/syntax, :attribute attr, :context at}))
    (when-not (or (ds/meta-attr? attr) (ds/schema-attr? attr) (ds/entity-spec-attr? attr))
      (if-let [db-idents (-> db :rschema :db/ident)]
        (let [attr (if (reverse-ref? attr)
                     (reverse-ref attr)
                     attr)]
          (when-not (db-idents attr)
            (raise "Bad entity attribute " attr " at " at ", not defined in current schema"
                   {:error :transact/schema :attribute attr :context at})))
        (raise "No schema found in db."
               {:error :transact/schema :attribute attr :context at})))))

(defn- validate-val [v [_ _ a _ _ :as at] db]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at}))
  (when (= :write (get-in db [:config :schema-flexibility]))
    (let [schema (:schema db)
          schema-spec (if (or (ds/meta-attr? a) (ds/schema-attr? a) (ds/entity-spec-attr? a)) ds/implicit-schema-spec schema)]
      (when-not (ds/value-valid? at schema)
        (raise "Bad entity value " v " at " at ", value does not match schema definition. Must be conform to: " (ds/describe-type (get-in schema-spec [a :db/valueType]))
               {:error :transact/schema :value v :attribute a :schema (get-in db [:schema a])})))))

(defn- current-tx [report]
  (inc (get-in report [:db-before :max-tx])))

(defn next-eid [db]
  (inc (:max-eid db)))

(defn- #?@(:clj  [^Boolean tx-id?]
           :cljs [^boolean tx-id?])
  [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx")                                ;; for datahike.js interop
      (= e "datomic.tx")
      (= e "datahike.tx")))

(defn- #?@(:clj  [^Boolean tempid?]
           :cljs [^boolean tempid?])
  [x]
  (or (and (number? x) (neg? x)) (string? x)))

(defn advance-max-eid [db eid]
  (cond-> db
    (and (> eid (:max-eid db))
         (< eid tx0))                                 ;; do not trigger advance if transaction id was referenced
    (assoc :max-eid eid)))

(defn advance-max-tid [db tid]
  (assoc db :max-tx tid))

(defn- allocate-eid
  ([report eid]
   (update-in report [:db-after] advance-max-eid eid))
  ([report e eid]
   (cond-> report
     (tx-id? e)
     (assoc-in [:tempids e] eid)
     (tempid? e)
     (assoc-in [:tempids e] eid)
     true
     (update-in [:db-after] advance-max-eid eid))))

(defn update-schema [db ^Datom datom]
  (let [schema (:schema db)
        e (.-e datom)
        a (.-a datom)
        v (.-v datom)]
    (if (= a :db/ident)
      (if (schema v)
        (raise (str "Schema with attribute " v " already exists")
               {:error :transact/schema :attribute v})
        (-> (assoc-in db [:schema v] (merge (or (schema e) {}) (hash-map a v)))
            (assoc-in [:schema e] v)))
      (if-let [schema-entry (schema e)]
        (if (schema schema-entry)
          (update-in db [:schema schema-entry a] (fn [old]
                                                   (if (ds/entity-spec-attr? a)
                                                     (if old
                                                       (conj old v)
                                                       [v])
                                                     v)))
          (assoc-in db [:schema e a] v))
        (assoc-in db [:schema e] (hash-map a v))))))

(defn update-rschema [db]
  (assoc db :rschema (rschema (:schema db))))

(defn remove-schema [db ^Datom datom]
  (let [schema (:schema db)
        e (.-e datom)
        a (.-a datom)
        v (.-v datom)]
    (if (= a :db/ident)
      (if-not (schema v)
        (raise (str "Schema with attribute " v " does not exist")
               {:error :retract/schema :attribute v})
        (-> (assoc-in db [:schema e] (dissoc (schema v) a))
            (update-in [:schema] #(dissoc % v))))
      (if-let [schema-entry (schema e)]
        (if (schema schema-entry)
          (update-in db [:schema schema-entry] #(dissoc % a))
          (update-in db [:schema e] #(dissoc % a v)))
        (raise (str "Schema with entity id " e " does not exist")
               {:error :retract/schema :entity-id e :attribute a :value e})))))



;; In context of `with-datom` we can use faster comparators which
;; do not check for nil (~10-15% performance gain in `transact`)


(defn- with-datom [db ^Datom datom]
  (ha/go-try
   (ha/<? (validate-datom db datom))
   (let [indexing? (indexing? db (.-a datom))
         a (.-a datom)
         schema? (or (ds/schema-attr? a) (ds/entity-spec-attr? a))
         keep-history? (and (-keep-history? db) (not (no-history? db a)))]
     (if (datom-added datom)
       (cond-> db
         true (ha/update-in< [:eavt] #(di/-insert % datom :eavt))
         true (ha/<?)
         true (ha/update-in< [:aevt] #(di/-insert % datom :aevt))
         true (ha/<?)
         indexing? (ha/update-in< [:avet] #(di/-insert % datom :avet))
         indexing? (ha/<?)
         true (advance-max-eid (.-e datom))
         true (update :hash + (hash datom))
         schema? (-> (update-schema datom)
                     update-rschema))
       (if-some [removing ^Datom (first (ha/<? (-search db [(.-e datom) (.-a datom) (.-v datom)])))]
         (cond-> db
           true (ha/update-in< [:eavt] #(di/-remove % removing :eavt))
           true (ha/<?)
           true (ha/update-in< [:aevt] #(di/-remove % removing :aevt))
           true (ha/<?)
           indexing? (ha/update-in< [:avet] #(di/-remove % removing :avet))
           indexing? (ha/<?)
           true (update :hash - (hash removing))
           schema? (-> (remove-schema datom) update-rschema)
           keep-history? (ha/update-in< [:temporal-eavt] #(di/-insert % removing :eavt))
           keep-history? (ha/<?)
           keep-history? (ha/update-in< [:temporal-eavt] #(di/-insert % datom :eavt))
           keep-history? (ha/<?)
           keep-history? (ha/update-in< [:temporal-aevt] #(di/-insert % removing :aevt))
           keep-history? (ha/<?)
           keep-history? (ha/update-in< [:temporal-aevt] #(di/-insert % datom :aevt))
           keep-history? (ha/<?)
           keep-history? (update :hash + (hash datom))
           (and keep-history? indexing?) (ha/update-in< [:temporal-avet] #(di/-insert % removing :avet))
           (and keep-history? indexing?) (ha/<?)
           (and keep-history? indexing?) (ha/update-in< [:temporal-avet] #(di/-insert % datom :avet))
           (and keep-history? indexing?) (ha/<?))
         db)))))

(defn- with-temporal-datom [db ^Datom datom] ;; TODO: do a take on this - was <??
  (ha/go-try
   (let [indexing? (indexing? db (.-a datom))
         schema? (ds/schema-attr? (.-a datom))
         current-datom ^Datom (first (ha/<? (-search db [(.-e datom) (.-a datom) (.-v datom)])))
         history-datom ^Datom (first (ha/<? (search-temporal-indices db [(.-e datom) (.-a datom) (.-v datom) (.-tx datom)])))
         current? (not (nil? current-datom))
         history? (not (nil? history-datom))]
     (cond-> db
       current? (update-in [:eavt] #(di/-remove % current-datom :eavt))
       current? (update-in [:aevt] #(di/-remove % current-datom :aevt))
       (and current? indexing?) (update-in [:avet] #(di/-remove % current-datom :avet))
       current? (update :hash - (hash current-datom))
       (and current? schema?) (-> (remove-schema datom) update-rschema)
       history? (update-in [:temporal-eavt] #(di/-remove % history-datom :eavt))
       history? (update-in [:temporal-aevt] #(di/-remove % history-datom :aevt))
       (and history? indexing?) (update-in [:temporal-avet] #(di/-remove % history-datom :avet))))))

(defn- transact-report [report datom]
  (ha/go-try
   (-> report
       (ha/update-in< [:db-after] with-datom datom)
       (ha/<?)
       (update-in [:tx-data] conj datom))))

(defn- check-upsert-conflict [entity acc]
  (let [[e a v] acc
        _e (:db/id entity)]
    (if (or (nil? _e)
            (tempid? _e)
            (nil? acc)
            (== _e e))
      acc
      (raise "Conflicting upsert: " [a v] " resolves to " e
             ", but entity already has :db/id " _e
             {:error     :transact/upsert
              :entity    entity
              :assertion acc}))))

(defn- upsert-eid [db entity]
  (when-let [idents (not-empty (-attrs-by db :db.unique/identity))]
    (ha/go-try
     (->>
      (ha/reduce<
       (fn [acc [a v]]                                       ;; acc = [e a v]
         (ha/go-try
          (if (contains? idents a)
            (do
              (validate-val v [nil nil a v nil] db)
              (if-some [e (:e (first (ha/<? (-datoms db :avet [a v]))))]
                (cond
                  (nil? acc) [e a v]                        ;; first upsert
                  (= (get acc 0) e) acc                     ;; second+ upsert, but does not conflict
                  :else
                  (let [[_e _a _v] acc]
                    (raise "Conflicting upserts: " [_a _v] " resolves to " _e
                           ", but " [a v] " resolves to " e
                           {:error     :transact/upsert
                            :entity    entity
                            :assertion [e a v]
                            :conflict  [_e _a _v]})))
                acc))                                       ;; upsert attr, but resolves to nothing
            acc)))                                           ;; non-upsert attr
       nil
       entity)
      (ha/<?)
      (check-upsert-conflict entity)
      first))))                                              ;; getting eid from acc


;; multivals/reverse can be specified as coll or as a single value, trying to guess
(defn- maybe-wrap-multival [db a vs]
  (cond
    ;; not a multival context
    (not (or (reverse-ref? a)
             (multival? db a)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (or (arrays/array? vs)
             (and (coll? vs) (not (map? vs)))))
    [vs]

    ;; probably lookup ref, but not an entity spec
    (and (= (count vs) 2)
         (is-attr? db (first vs) :db.unique/identity)
         (not (ds/entity-spec-attr? a)))
    [vs]

    :else vs))

(defn- explode [db entity]
  (let [eid (:db/id entity)
        ensure (:db/ensure entity)
        entities (for [[a vs] entity
                       :when (not (or (= a :db/id) (= a :db/ensure)))
                       :let [_ (validate-attr a {:db/id eid, a vs} db)
                             reverse? (reverse-ref? a)
                             straight-a (if reverse? (reverse-ref a) a)
                             _ (when (and reverse? (not (ref? db straight-a)))
                                 (raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                                        {:error :transact/syntax, :attribute a, :context {:db/id eid, a vs}}))]
                       v (maybe-wrap-multival db a vs)]
                   (if (and (ref? db straight-a) (map? v))               ;; another entity specified as nested map
                     (assoc v (reverse-ref a) eid)
                     (if reverse?
                       [:db/add v straight-a eid]
                       [:db/add eid straight-a v])))]
    (if ensure
      (let [{:keys [:db.entity/attrs :db.entity/preds]} (-> db :schema ensure)]
        (if (empty? attrs)
          (if (empty? preds)
            entities
            (concat entities [[:db.ensure/preds eid ensure preds]]))
          (if (empty? preds)
            (concat entities [[:db.ensure/attrs eid ensure attrs]])
            (concat entities [[:db.ensure/attrs eid ensure attrs]
                              [:db.ensure/preds eid ensure preds]]))))
      entities)))

(defn- get-datom-value [^Datom old-datom]
  (when old-datom (.-v old-datom)))

(defn- transact-add [{{{:keys [keep-history?]} :config :as db-after} :db-after :as report} [_ e a v tx :as ent]]
  (validate-attr a ent db-after)
  (validate-val v ent db-after)
  (ha/go-try
   (let [tx (or tx (current-tx report))
         db (:db-after report)
         e (ha/<? (entid-strict db e))
         v (if (ref? db a) (ha/<? (entid-strict db v)) v)
         new-datom (datom e a v tx)]
     (if (multival? db a)
       (if (empty? (ha/<? (-search db [e a v])))
         (ha/<? (transact-report report new-datom))
         report)
       (let [old-datom (first (ha/<? (-search db [e a])))  ;;TODO: review type hints in core.async or open bug report
             ov (get-datom-value old-datom)]
         (if old-datom
           (if (= ov v)
             report
             (-> report
                 (transact-report (datom e a ov tx false))
                 (ha/<?)
                 (transact-report new-datom)
                 (ha/<?)))
           (ha/<? (transact-report report new-datom))))))))

(defn- transact-retract-datom [report ^Datom d]
  (transact-report report (datom (.-e d) (.-a d) (.-v d) (current-tx report) false)))

(defn- transact-purge-datom [report ^Datom d]
  (let [tx (current-tx report)]
    report
    #_(update-in report [:db-after] with-temporal-datom d)))

(defn- retract-components [db datoms]
  (into #{} (comp
             (filter (fn [^Datom d] (component? db (.-a d))))
             (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

(defn- purge-components [db datoms]
  (let [xf (comp
            (filter (fn [^Datom d] (component? db (.-a d))))
            (map (fn [^Datom d] [:db.purge/entity (.-v d)])))]
    (into #{} xf datoms)))

#?(:clj
   (defmacro cond+ [& clauses]
     (when-some [[test expr & rest] clauses]
       (case test
         :let `(let ~expr (cond+ ~@rest))
         `(if ~test ~expr (cond+ ~@rest))))))

#?(:clj
   (defmacro some-of
     ([] nil)
     ([x] x)
     ([x & more]
      `(let [x# ~x] (if (nil? x#) (some-of ~@more) x#)))))

(declare transact-tx-data)

(defn- retry-with-tempid [initial-report report es tempid upserted-eid]
  (if (contains? (:tempids initial-report) tempid)
    (raise "Conflicting upsert: " tempid " resolves"
           " both to " upserted-eid " and " (get-in initial-report [:tempids tempid])
           {:error :transact/upsert})
    ;; try to re-run from the beginning
    ;; but remembering that `tempid` will resolve to `upserted-eid`
    (let [tempids' (-> (:tempids report)
                       (assoc tempid upserted-eid))
          report' (assoc initial-report :tempids tempids')]
      (transact-tx-data report' es))))

(defn assert-preds [db [_ e _ preds]]
  (reduce
   (fn [coll pred]
     (if (#?(:clj (resolve pred)
             :cljs (resolve `pred)) db e)
       coll
       (conj coll pred)))
   #{} preds))

(def builtin-fn?
  #{:db.fn/call
    :db.fn/cas
    :db/cas
    :db/add
    :db/retract
    :db.fn/retractAttribute
    :db.fn/retractEntity
    :db/retractEntity
    :db/purge
    :db.ensure/attrs
    :db.ensure/preds
    :db.purge/entity
    :db.purge/attribute
    :db.history.purge/before})

(defn purge [db report entities op entity [e a v]]
  (async/go
    (if (-keep-history? db)
      (let [history (HistoricalDB. db)]
        (if-some [e (ha/<? (entid history e))]
          (let [v (if (ref? history a) (ha/<? (entid-strict history v)) v)
                old-datoms (ha/<? (-search history [e a v]))]
            [(reduce transact-purge-datom report old-datoms) entities])
          (raise "Can't find entity with ID " e " to be purged" {:error :transact/purge, :operation op, :tx-data entity})))
      (raise "Purge is only available in temporal databases." {:error :transact/purge :operation op :tx-data entity}))))

(defn purge-attribute [db report entities op entity [e a _]]
  (async/go
    (if (-keep-history? db)
      (let [history (HistoricalDB. db)]
        (if-let [e (ha/<? (entid history e))]
          (let [datoms (vec (ha/<? (-search history [e a])))]
            [(reduce transact-purge-datom report datoms)
             (concat (purge-components history datoms) entities)])
          (raise "Can't find entity with ID " e " to be purged" {:error :transact/purge, :operation op, :tx-data entity})))
      (raise "Purge attribute is only available in temporal databases." {:error :transact/purge :operation op :tx-data entity}))))

(defn purge-entity [db report entities op entity [e a _]]
  (if (-keep-history? db)
    (let [history (HistoricalDB. db)]
      (if-let [e (ha/<? (entid history e))]
        (let [e-datoms (vec (ha/<? (-search history [e])))
              v-datoms (vec (mapcat (fn [a] (ha/<? (-search history [nil a e]))) (-attrs-by history :db.type/ref)))
              retracted-comps (purge-components history e-datoms)]
          [(reduce transact-purge-datom report (concat e-datoms v-datoms))
           (concat retracted-comps entities)])
        (raise "Can't find entity with ID " e " to be purged" {:error :transact/purge, :operation op, :tx-data entity})))
    (raise "Purge entity is only available in temporal databases." {:error :transact/purge :operation op :tx-data entity})))

(defn purge-before [db report entities op entity [e _ _]]
  (async/go
    (if (-keep-history? db)
      (let [history (HistoricalDB. db)
            e-datoms (-> (ha/<? (search-temporal-indices db nil))
                         vec
                         (filter-before e db)
                         (ha/<?)
                         vec)
            retracted-comps (purge-components history e-datoms)]
        [(reduce transact-purge-datom report e-datoms)
         (concat retracted-comps entities)])
      (raise "Purge entity is only available in temporal databases." {:error :transact/purge :operation op :tx-data entity}))))

(defn transact-tx-data [initial-report initial-es]
  (when-not (or (nil? initial-es)
                (sequential? initial-es))
    (raise "Bad transaction data " initial-es ", expected sequential collection"
           {:error :transact/syntax, :tx-data initial-es}))
  (async/go
    (loop [report (update initial-report :db-after transient)
           es (if (-keep-history? (get-in initial-report [:db-before]))
                (concat [[:db/add (current-tx report) :db/txInstant (get-time) (current-tx report)]] initial-es)
                initial-es)]
      (let [[entity & entities] es
            db (:db-after report)
            {:keys [tempids]} report]
        (cond
          (empty? es)
          (-> report
              (assoc-in [:tempids :db/current-tx] (current-tx report))
              (update-in [:db-after :max-tx] inc)
              (update :db-after persistent!))

          (nil? entity)
          (recur report entities)

          (map? entity)
          (let [old-eid (:db/id entity)]
            (cond+
            ;; :db/current-tx / "datomic.tx" => tx
             (tx-id? old-eid)
             (let [id (current-tx report)]
               (recur (allocate-eid report old-eid id)
                      (cons (assoc entity :db/id id) entities)))

            ;; lookup-ref => resolved | error
             (sequential? old-eid)
             (let [id (ha/<? (entid-strict db old-eid))]
               (recur report
                      (cons (assoc entity :db/id id) entities)))

            ;; upserted => explode | error
             :let [upserted-eid (ha/<? (upsert-eid db entity))]

             (some? upserted-eid)
             (if (and (tempid? old-eid)
                      (contains? tempids old-eid)
                      (not= upserted-eid (get tempids old-eid)))
               (retry-with-tempid initial-report report initial-es old-eid upserted-eid)
               (do
                ;; schema tx
                 (when (ds/schema-entity? entity)
                   (if-let [attr-name (get-in db [:schema upserted-eid])]
                     (when-let [invalid-updates (ds/find-invalid-schema-updates entity (get-in db [:schema attr-name]))]
                       (when-not (empty? invalid-updates)
                         (raise "Update not supported for these schema attributes"
                                {:error :transact/schema :entity entity :invalid-updates invalid-updates})))
                     (when (= :write (get-in db [:config :schema-flexibility]))
                       (when (or (:db/cardinality entity) (:db/valueType entity))
                         (when-not (ds/schema? entity)
                           (raise "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
                                  {:error :transact/schema :entity entity}))))))
                 (recur (allocate-eid report old-eid upserted-eid)
                        (concat (explode db (assoc entity :db/id upserted-eid)) entities))))

            ;; resolved | allocated-tempid | tempid | nil => explode
             (or (number? old-eid)
                 (nil? old-eid)
                 (string? old-eid))
             (let [new-eid (cond
                             (nil? old-eid) (next-eid db)
                             (tempid? old-eid) (or (get tempids old-eid)
                                                   (next-eid db))
                             :else old-eid)
                   new-entity (assoc entity :db/id new-eid)]
               (when (ds/schema-entity? entity)
                 (if-let [attr-name (get-in db [:schema new-eid])]
                   (when-let [invalid-updates (ds/find-invalid-schema-updates entity (get-in db [:schema attr-name]))]
                     (when-not (empty? invalid-updates)
                       (raise "Update not supported for these schema attributes"
                              {:error :transact/schema :entity entity :invalid-updates invalid-updates})))
                   (when (= :write (get-in db [:config :schema-flexibility]))
                     (when (or (:db/cardinality entity) (:db/valueType entity))
                       (when-not (ds/schema? entity)
                         (raise "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
                                {:error :transact/schema :entity entity}))))))
               (recur (allocate-eid report old-eid new-eid)
                      (concat (explode db new-entity) entities)))

            ;; trash => error
             :else
             (raise "Expected number, string or lookup ref for :db/id, got " old-eid
                    {:error :entity-id/syntax, :entity entity})))

          (sequential? entity)
          (let [[op e a v] entity]
            (cond
              (= op :db.fn/call)
              (let [[_ f & args] entity]
                (recur report (concat (apply f db args) entities)))

              (and (keyword? op)
                   (not (builtin-fn? op)))
              (if-some [ident (ha/<? (entid db op))]
                (let [fun (-> (ha/<? (-search db [ident :db/fn])) first :v)
                      args (next entity)]
                  (if (fn? fun)
                    (recur report (concat (apply fun db args) entities))
                    (raise "Entity " op " expected to have :db/fn attribute with fn? value"
                           {:error :transact/syntax, :operation :db.fn/call, :tx-data entity})))
                (raise "Can???t find entity for transaction fn " op
                       {:error :transact/syntax, :operation :db.fn/call, :tx-data entity}))

              (and (tempid? e) (not= op :db/add))
              (raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
                     {:error :transact/syntax, :op entity})

              (or (= op :db.fn/cas)
                  (= op :db/cas))
              (let [[_ e a ov nv] entity
                    e (ha/<? (entid-strict db e))
                    _ (validate-attr a entity db)
                    nv (if (ref? db a) (ha/<? (entid-strict db nv)) nv)
                    datoms (ha/<? (-search db [e a]))]
                (if (nil? ov)
                  (if (empty? datoms)
                    (recur (ha/<? (transact-add report [:db/add e a nv])) entities)
                    (raise ":db.fn/cas failed on datom [" e " " a " " (if (multival? db a) (map :v datoms) (:v (first datoms))) "], expected nil"
                           {:error :transact/cas, :old (if (multival? db a) datoms (first datoms)), :expected ov, :new nv}))
                  (let [ov (if (ref? db a) (ha/<? (entid-strict db ov)) ov)
                        _ (validate-val nv entity db)]
                    (if (multival? db a)
                      (if (some (fn [^Datom d] (= (.-v d) ov)) datoms)
                        (recur (ha/<? (transact-add report [:db/add e a nv])) entities)
                        (raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                               {:error :transact/cas, :old datoms, :expected ov, :new nv}))
                      (let [v (:v (first datoms))]
                        (if (= v ov)
                          (recur (ha/<? (transact-add report [:db/add e a nv])) entities)
                          (raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                                 {:error :transact/cas, :old (first datoms), :expected ov, :new nv})))))))

              (tx-id? e)
              (recur (allocate-eid report e (current-tx report)) (cons [op (current-tx report) a v] entities))

              (and (ref? db a) (tx-id? v))
              (recur (allocate-eid report v (current-tx report)) (cons [op e a (current-tx report)] entities))

              (tempid? e)
              (let [upserted-eid (when (is-attr? db a :db.unique/identity)
                                   (:e (first (ha/<? (-datoms db :avet [a v])))))
                    allocated-eid (get tempids e)]
                (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                  (retry-with-tempid initial-report report initial-es e upserted-eid)
                  (let [eid (or upserted-eid allocated-eid (next-eid db))]
                    (recur (allocate-eid report e eid) (cons [op eid a v] entities)))))

              (and (ref? db a) (tempid? v))
              (if-let [vid (get tempids v)]
                (recur report (cons [op e a vid] entities))
                (recur (allocate-eid report v (next-eid db)) es))

              (= op :db/add)
              (recur (ha/<? (transact-add report entity)) entities)

              (= op :db/retract)
              (if-some [e (ha/<? (entid db e))]
                (let [v (if (ref? db a) (ha/<? (entid-strict db v)) v)]
                  (validate-attr a entity db)
                  (validate-val v entity db)
                  (if-some [old-datom (first (ha/<? (-search db [e a v])))]
                    (recur (ha/<? (transact-retract-datom report old-datom)) entities)
                    (recur report
                           entities)))
                (recur report entities))

              (= op :db.fn/retractAttribute)
              (if-let [e (ha/<? (entid db e))]
                (let [_ (validate-attr a entity db)
                      datoms (vec (ha/<? (-search db [e a])))]
                  (recur (ha/<? (ha/reduce< transact-retract-datom report datoms))
                         (concat (retract-components db datoms) entities)))
                (recur report entities))

              (or (= op :db.fn/retractEntity)
                  (= op :db/retractEntity))
              (do
                ;(js/console.log ":db.fn/retractEntity called")
                (if-let [e (ha/<? (entid db e))]
                  (let [e-datoms (vec (ha/<? (-search db [e])))
                        v-datoms (vec (apply concat (ha/<? (ha/map< (fn [a] (ha/go-try (ha/<? (-search db [nil a e])))) (-attrs-by db :db.type/ref)))))
                        retracted-comps (retract-components db e-datoms)]
                    (recur (ha/<? (ha/reduce< transact-retract-datom report (concat e-datoms v-datoms)))
                           (concat retracted-comps entities)))
                  (recur report entities)))

              (= op :db/purge)
              (when-let [[report es] (purge db report entities op entity [e a v])]
                (recur report es))

              (= op :db.purge/attribute)
              (when-let [[report es] (purge-entity db report entities op entity [e a v])]
                (recur report es))

              (= op :db.purge/entity)
              (when-let [[report es] (purge-entity db report entities op entity [e a v])]
                (recur report es))

              (= op :db.history.purge/before)
              (when-let [[report es] (purge-before db report entities op entity [e a v])]
                (recur report es))


            ;; assert required attributes


              (= op :db.ensure/attrs)
              (let [{:keys [tx-data]} report
                    asserting-datoms (filter (fn [^Datom d] (= e (.-e d))) tx-data)
                    asserting-attributes (map (fn [^Datom d] (.-a d)) asserting-datoms)
                    diff (clojure.set/difference (set v) (set asserting-attributes))]
                (if (empty? diff)
                  (recur report entities)
                  (raise "Entity " e " missing attributes " diff " of spec " a
                         {:error :transact/ensure
                          :operation op
                          :tx-data entity
                          :asserting-datoms asserting-datoms})))

            ;; assert entity predicates
              (= op :db.ensure/preds)
              (let [{:keys [db-after]} report
                    preds (assert-preds db-after entity)]
                (if-not (empty? preds)
                  (raise "Entity " e " failed predicates " preds " of spec " a
                         {:error :transact/ensure
                          :operation op
                          :tx-data entity})
                  (recur report entities)))

              :else
              (raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute, :db.fn/retractEntity or an ident corresponding to an installed transaction function (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)" {:error :transact/syntax, :operation op, :tx-data entity})))

          (datom? entity)
          (let [[e a v tx added] entity]
            (if added
              (recur (ha/<? (transact-add report [:db/add e a v tx])) entities)
              (recur report (cons [:db/retract e a v] entities))))

          :else
          (raise "Bad entity type at " entity ", expected map or vector"
                 {:error :transact/syntax, :tx-data entity}))))))

(defn transact-entities-directly [initial-report initial-es]
  (ha/go-try                                                   ;; TODO: do a take on this at all call sites
   (loop [report (update initial-report :db-after persistent!)
          es initial-es
          migration-state (or (get-in initial-report [:db-before :migration]) {})]
     (let [[entity & entities] es
           db (:db-after report)
           [e a v t op] entity
           max-eid (next-eid db)
           max-tid (inc (get-in report [:db-after :max-tx]))]
       (cond
         (empty? es)
         (-> report
             (update-in [:db-after :max-tx] inc)
             (update-in [:db-after :migration] #(if %
                                                  (merge % migration-state)
                                                  migration-state))
             (update :db-after persistent!))

         (= :db.install/attribute a)
         (recur report entities migration-state)

        ;; meta entity
         (ds/meta-attr? a)
         (let [new-datom (dd/datom max-tid a v max-tid op)
               new-e (.-e new-datom)]
           (recur (-> (ha/<? (transact-report report new-datom))
                      (assoc-in [:db-after :max-tx] max-tid))
                  entities
                  (-> migration-state
                      (assoc-in [:tids e] new-e)
                      (assoc-in [:eids e] new-e))))

        ;; ref not added yet
         (and (ref? db a) (nil? (get-in migration-state [:eids v])))
         (recur (allocate-eid report max-eid) es (assoc-in migration-state [:eids v] max-eid))

         :else
         (let [new-datom ^Datom (dd/datom
                                 (or (get-in migration-state [:eids e]) max-eid)
                                 a
                                 (if (ref? db a)
                                   (get-in migration-state [:eids v])
                                   v)
                                 (get-in migration-state [:tids t])
                                 op)]
           (recur (ha/<? (transact-report report new-datom)) entities (assoc-in migration-state [:eids e] (.-e new-datom)))))))))
