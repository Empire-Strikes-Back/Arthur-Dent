(ns konserve.orbitdb
  (:require
   [clojure.core.async :as a
    :refer [chan put! take! close! offer! to-chan! timeout poll!
            sliding-buffer dropping-buffer
            go >! <! alt! alts! do-alts
            mult tap untap pub sub unsub mix unmix admix
            pipe pipeline pipeline-async]]
   [clojure.string]
   [cljs.core.async.impl.protocols :refer [closed?]]
   [cljs.core.async.interop :refer-macros [<p!]]
   [goog.string.format]
   [goog.string :refer [format]]
   [goog.object]
   [cljs.reader :refer [read-string]]

   [incognito.edn]
   [konserve.core]
   [konserve.serializers]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore -exists? -get -update-in -assoc-in -get-meta
                               PBinaryAsyncKeyValueStore -bget -bassoc
                               PStoreSerializer -serialize -deserialize]]))

(defonce orbitdbA (atom nil))

(defrecord OrbitDBKeyValueStore [db db-name serializer read-handlers write-handlers locks version]
  PEDNAsyncKeyValueStore
  (-exists?
   [this key]
   (let [out| (chan 1)]
     (try
       (put! out| (not (== (.-length (.get db (pr-str key))) 0)))
       (close! out|)
       (catch js/Error ex
         (put! out| (ex-info "Cannot check for existence."
                             {:type  :access-error
                              :key   key
                              :error ex}))
         (close! out|)))
     out|))

  (-get-meta
   [this key]
   (let [out| (chan 1)
         values (.get db (pr-str key))]
     (if (== (.-length values) 0)
       (let []
         (put! out| (ex-info "-get-meta Cannot read edn value."
                             {:type  :read-error
                              :key   key
                              :error (js/Error. "no value under key")}))
         (close! out|))
       (let [value (first values)]
         (put! out| (-deserialize serializer read-handlers (aget value "meta")))
         (close! out|)))
     out|))

  (-get
   [this key]
   (let [out| (chan 1)
         values (.get db (pr-str key))]
     (if (== (.-length values) 0)
       (let []
         #_(put! out| (ex-info "-get Cannot read edn value."
                               {:type  :read-error
                                :key   key
                                :error (js/Error. "no value under key")}))
         (close! out|))
       (let [value (first values)]
         (put! out| (-deserialize serializer read-handlers (aget value "edn_value")))
         (close! out|)))
     out|))

  (-assoc-in
   [this key-vec meta-up val]
   (-update-in this key-vec meta-up (fn [_] val) []))
  (-update-in
   [this key-vec meta-up up-fn args]
   (let [[fkey & rkey] key-vec
         out| (chan 1)
         values (.get db (pr-str fkey))]
     (if false #_(== (.-length values) 0)
         (let []
           (put! out| (ex-info "-update-in Cannot read edn value."
                               {:type  :read-error
                                :key   key
                                :error (js/Error. "no value under key")}))
           (close! out|))
         (try
           (let [[old-meta old] (when-let [value (first values)]
                                  [(-deserialize serializer read-handlers (aget value "meta")) (-deserialize serializer read-handlers (aget value "edn_value"))])
                 edn-meta (meta-up old-meta)
                 edn-value (if-not (empty? rkey)
                             (apply update-in old rkey up-fn args)
                             (apply up-fn old args))]
             (->
              (.put db (clj->js {:key (pr-str fkey)
                                 :version version
                                 :meta (-serialize serializer nil write-handlers edn-meta)
                                 :edn_value (-serialize serializer nil write-handlers edn-value)}))
              (.then (fn [multihash]
                       (put! out| [(get-in old rkey) edn-value])
                       (close! out|)))
              (.catch (fn [ex]
                        (put! out| (ex-info "-update-in Cannot parse edn value."
                                            {:type  :read-error
                                             :key   key-vec
                                             :error ex}))
                        (close! out|)))))
           (catch :default ex
             (put! out| (ex-info "-update-in Cannot parse edn value."
                                 {:type  :read-error
                                  :key   key-vec
                                  :error ex}))
             (close! out|))))
     out|))

  (-dissoc [this key]
           (let [out| (chan 1)]
             (->
              (.del db (pr-str key))
              (.then (fn [multihash]
                       (close! out|)))
              (.catch (fn [ex]
                        (put! out| (ex-info "-dissoc Cannot write edn value."
                                            {:type  :write-error
                                             :key   key
                                             :error ex}))
                        (close! out|))))
             out|))

  PBinaryAsyncKeyValueStore
  (-bget [this key lock-cb]
    (let [out| (chan 1)
          values (.get db (pr-str key))]
      (if (== (.-length values) 0)
        (go
          (>! out| (<!
                    (lock-cb (ex-info "-bget Cannot read binary value."
                                      {:type  :read-error
                                       :key   key
                                       :error (js/Error. "no value under key")}))))
          (close! out|))
        (let [value (first values)]
          (put! out| (lock-cb (aget value "value")))
          (close! out|)))
      out|))

  (-bassoc [this key meta-up blob]
    (let [out| (chan 1)
          values (.get db (pr-str key))]
      (if (== (.-length values) 0)
        (let []
          (put! out| (ex-info "-bassoc Cannot read binary value."
                              {:type  :read-error
                               :key   key
                               :error (js/Error. "no value under key")}))
          (close! out|))
        (try
          (let [old-meta (when-let [value (first values)]
                           (-deserialize serializer read-handlers (aget value "meta")))
                edn-meta (meta-up old-meta)]
            (->
             (.put db
                   (clj->js {:key   (pr-str key)
                             :meta (-serialize serializer nil write-handlers edn-meta)
                             :version version
                             :value blob}))
             (.then (fn [multihash]
                      (close! out|)))
             (.catch (fn [ex]
                       (put! out| (ex-info "-bassoc Cannot write binary value."
                                           {:type  :write-error
                                            :key   key
                                            :error ex}))
                       (close! out|)))))
          (catch :default ex
            (put! out| (ex-info "-bassoc Cannot parse edn value."
                                {:type  :read-error
                                 :key   key
                                 :error ex}))
            (close! out|))))
      out|)))

(defn new-store
  "Create an OrbitDB backed edn store with read-handlers according to
  incognito.

  Be careful not to mix up edn and JSON values."
  [name & {:keys [read-handlers write-handlers serializer version]
           :or {read-handlers (atom {})
                write-handlers (atom {})
                serializer (konserve.serializers/string-serializer)
                version 1}}]
  (let [out| (chan 1)]
    (->
     (.docs @orbitdbA name (clj->js {:indexBy "key"}))
     (.then (fn [store]
              (->
               (.load store)
               (.then (fn []
                        store)))))
     (.then (fn [store]
              (put! out| (map->OrbitDBKeyValueStore
                          {:db store
                           :db-name name
                           :serializer serializer
                           :read-handlers read-handlers
                           :write-handlers write-handlers
                           :locks (atom {})
                           :version version}))))
     (.catch (fn [ex]
               (put! out| (ex-info "Cannot open OrbitDB store."
                                   {:type :db-error
                                    :error ex}))
               (close! out|))))
    out|))


(defn delete-store
  "Delete an OrbitDB backed."

  [{:keys [id]}]
  (let [out| (chan 1)]
    (->
     (.docs @orbitdbA id (clj->js {:indexBy "key"}))
     (.then (fn [store]
              (.drop store)))
     (.then (fn []
              (put! out| true)))
     (.catch (fn [ex]
               (put! out| (ex-info "Cannot delete OrbitDB store."
                                   {:type :db-error
                                    :id id
                                    :error ex}))
               (close! out|))))
    out|))

(defn store-exists?
  [config]
  (let [{:keys [id]} (:store config)]
    (go
      (try
        (let [orbitdb @orbitdbA
              store-address (<p! (._determineAddress orbitdb id "docstore" (clj->js {:indexBy "key"})))
              cache (<p! (._requestCache orbitdb (.toString store-address) (.-directory orbitdb)))
              store-exists? (<p! (._haveLocalData orbitdb cache store-address))]
          store-exists?)
        (catch js/Error err (js/console.log (ex-cause err)))))))

(comment
    ;;new-gc
    ;; jack in figwheel cljs REPL
  (require 'figwheel-sidecar.repl-api)
  (figwheel-sidecar.repl-api/cljs-repl)

  (defrecord Test [a])
  (Test. 5)

  (go (def my-store (<! (new-indexeddb-store "konserve"
                                             :read-handlers
                                             (atom {'konserve.indexeddb.Test
                                                    map->Test})))))

    ;; or
  (-jassoc-in my-store ["test" "bar"] #js {:a 3})
  (go (println (<! (-jget-in my-store ["test"]))))
  (go (println (<! (-exists? my-store 1))))

  (go (doseq [i (range 10)]
        (println (<! (-get-in my-store [i])))))

  (go (time
       (doseq [i (range 10)]
         (<! (-update-in my-store [i] (fn [_] (inc i)))))
       #_(doseq [i (range 10)]
           (println (<! (-get-in my-store [i]))))))
  (go (println (<! (-get my-store 999))))

  (go (prn (<! (-update-in my-store ["foo"] (fn [_] {:meta "META"}) (fn [_] 0) []))))

  (go (prn (<! (-update-in my-store ["foo"] (fn [_] {:meta "META"}) inc []))))

  (go (println (<! (-get-meta my-store "foo"))))

  (go (println (<! (-get my-store "foo"))))

  (go (println (<! (-assoc-in my-store ["rec-test"] (Test. 5)))))
  (go (println (<! (-get my-store "rec-test"))))

  (go (println (<! (-assoc-in my-store ["test2"] {:a 1 :b 4.2}))))

  (go (println (<! (-assoc-in my-store ["test"] {:a 43}))))

  (go (println (<! (-update-in my-store ["test" :a] inc))))
  (go (println (<! (-get my-store "test2"))))

  (go (println (<! (-bassoc my-store
                            "blob-fun"
                            (fn [_] "my meta")
                            (new js/Blob #js ["hello worlds"], #js {"type" "text/plain"})))))

  (go (println (<! (-get-meta my-store "blob-fun"))))

  (go (.log js/console (<! (-bget my-store "blob-fun" identity)))))
