(ns ^:no-doc datahike.pull-api
  (:require
   [datahike.db :as db]
   [clojure.core.async :as async :refer [go <!]]
   [hitchhiker.tree.utils.cljs.async :as ha]
   #?(:cljs [datahike.datom :refer [Datom]])
   #?(:cljs [datalog.parser.pull :refer [PullSpec]])
   [datalog.parser.pull :as dpp])
  #?(:clj
     (:import
      [datahike.datom Datom]
      [datalog.parser.pull PullSpec])))

(defn- into!
  [transient-coll items]
  (reduce conj! transient-coll items))

(def ^:private ^:const +default-limit+ 1000)

(defn- initial-frame
  [pattern eids multi?]
  {:state     :pattern
   :pattern   pattern
   :wildcard? (:wildcard? pattern)
   :specs     (-> pattern :attrs seq)
   :results   (transient [])
   :kvps      (transient {})
   :eids      eids
   :multi?    multi?
   :recursion {:depth {} :seen #{}}})

(defn- subpattern-frame
  [pattern eids multi? attr]
  (assoc (initial-frame pattern eids multi?) :attr attr))

(defn- reset-frame
  [frame eids kvps]
  (let [pattern (:pattern frame)]
    (assoc frame
           :eids      eids
           :specs     (seq (:attrs pattern))
           :wildcard? (:wildcard? pattern)
           :kvps      (transient {})
           :results   (cond-> (:results frame)
                        (seq kvps) (conj! kvps)))))

(defn- push-recursion
  [rec attr eid]
  (let [{:keys [depth seen]} rec]
    (assoc rec
           :depth (update depth attr (fnil inc 0))
           :seen (conj seen eid))))

(defn- seen-eid?
  [frame eid]
  (-> frame
      (get-in [:recursion :seen] #{})
      (contains? eid)))

(defn- pull-seen-eid
  [frame frames eid]
  (when (seen-eid? frame eid)
    (conj frames (update frame :results conj! {:db/id eid}))))

(defn- single-frame-result
  [key frame]
  (some-> (:kvps frame) persistent! (get key)))

(defn- recursion-result [frame]
  (single-frame-result ::recursion frame))

(defn- recursion-frame
  [parent eid]
  (let [attr (:attr parent)
        rec  (push-recursion (:recursion parent) attr eid)]
    (assoc (subpattern-frame (:pattern parent) [eid] false ::recursion)
           :recursion rec)))

(defn- pull-recursion-frame
  [db [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (let [frame  (reset-frame frame (rest eids) (recursion-result frame))
          eid    (first eids)]
      (or (pull-seen-eid frame frames eid)
          (conj frames frame (recursion-frame frame eid))))
    (let [kvps    (recursion-result frame)
          results (cond-> (:results frame)
                    (seq kvps) (conj! kvps))]
      (conj frames (assoc frame :state :done :results results)))))

(defn- recurse-attr
  [db attr multi? eids eid parent frames]
  (let [{:keys [recursion pattern]} parent
        depth  (-> recursion (get :depth) (get attr 0))]
    (if (-> pattern :attrs (get attr) :recursion (= depth))
      (conj frames parent)
      (pull-recursion-frame
       db
       (conj frames parent
             {:state :recursion :pattern pattern
              :attr attr :multi? multi? :eids eids
              :recursion recursion
              :results (transient [])})))))

(let [pattern (PullSpec. true {})]
  (defn- expand-frame
    [parent eid attr-key multi? eids]
    (let [rec (push-recursion (:recursion parent) attr-key eid)]
      (-> pattern
          (subpattern-frame eids multi? attr-key)
          (assoc :recursion rec)))))

(defn- pull-attr-datoms
  [db attr-key attr eid forward? datoms opts [parent & frames]]
  (let [limit (get opts :limit +default-limit+)
        attr-key (or (:as opts) attr-key)
        found (not-empty
               (cond->> datoms
                 limit (into [] (take limit))))]
    (if found
      (let [ref?       (db/ref? db attr)
            component? (and ref? (db/component? db attr))
            multi?     (if forward? (db/multival? db attr) (not component?))
            datom-val  (if forward? (fn [d] (.-v ^Datom d)) (fn [d] (.-e ^Datom d)))]
        (cond
          (contains? opts :subpattern)
          (->> (subpattern-frame (:subpattern opts)
                                 (mapv datom-val found)
                                 multi? attr-key)
               (conj frames parent))

          (contains? opts :recursion)
          (recurse-attr db attr-key multi?
                        (mapv datom-val found)
                        eid parent frames)

          (and component? forward?)
          (->> found
               (mapv datom-val)
               (expand-frame parent eid attr-key multi?)
               (conj frames parent))

          :else
          (let [as-value  (cond->> datom-val
                            ref? (comp #(hash-map :db/id %)))
                single?   (not multi?)]
            (->> (cond-> (into [] (map as-value) found)
                   single? first)
                 (update parent :kvps assoc! attr-key)
                 (conj frames)))))
      (->> (cond-> parent
             (contains? opts :default)
             (update :kvps assoc! attr-key (:default opts)))
           (conj frames)))))

(defn- pull-attr
  [db spec eid frames]
  (ha/go-try
   (let [[attr-key opts] spec]
     (if (= :db/id attr-key)
       (if (not-empty (ha/<? (db/-datoms db :eavt [eid])))
         (conj (rest frames)
               (update (first frames) :kvps assoc! :db/id eid))
         frames)
       (let [attr     (:attr opts)
             forward? (= attr-key attr)
             results  (if forward?
                        (ha/<? (db/-datoms db :eavt [eid attr]))
                        (ha/<? (db/-datoms db :avet [attr eid])))]
         (pull-attr-datoms db attr-key attr eid forward?
                           results opts frames))))))

(def ^:private filter-reverse-attrs
  (filter (fn [[k v]] (not= k (:attr v)))))

(defn- expand-reverse-subpattern-frame
  [parent eid rattrs]
  (-> (:pattern parent)
      (assoc :attrs rattrs :wildcard? false)
      (subpattern-frame [eid] false ::expand-rev)))

(defn- expand-result
  [frames kvps]
  (->> kvps
       (persistent!)
       (update (first frames) :kvps into!)
       (conj (rest frames))))

(defn- pull-expand-reverse-frame
  [db [frame & frames]]
  (->> (or (single-frame-result ::expand-rev frame) {})
       (into! (:expand-kvps frame))
       (expand-result frames)))

(defn- pull-expand-frame
  [db [frame & frames]]
  (if-let [datoms-by-attr (seq (:datoms frame))]
    (let [[attr datoms] (first datoms-by-attr)
          opts          (-> frame
                            (get-in [:pattern :attrs])
                            (get attr {}))]
      (pull-attr-datoms db attr attr (:eid frame) true datoms opts
                        (conj frames (update frame :datoms rest))))
    (if-let [rattrs (->> (get-in frame [:pattern :attrs])
                         (into {} filter-reverse-attrs)
                         not-empty)]
      (let [frame  (assoc frame
                          :state       :expand-rev
                          :expand-kvps (:kvps frame)
                          :kvps        (transient {}))]
        (->> rattrs
             (expand-reverse-subpattern-frame frame (:eid frame))
             (conj frames frame)))
      (expand-result frames (:kvps frame)))))

(defn- pull-wildcard-expand
  [db frame frames eid pattern]
  (ha/go-try
   (let [datoms (group-by (fn [d] (.-a ^Datom d)) (ha/<? (db/-datoms db :eavt [eid])))
         {:keys [attr recursion]} frame
         rec (cond-> recursion
               (some? attr) (push-recursion attr eid))]
     (->> {:state :expand :kvps (transient {:db/id eid})
           :eid eid :pattern pattern :datoms (seq datoms)
           :recursion rec}
          (conj frames frame)
          (pull-expand-frame db)))))

(defn- pull-wildcard
  [db frame frames]
  (ha/go-try
   (let [{:keys [eid pattern]} frame]
     (or (pull-seen-eid frame frames eid)
         (ha/<? (pull-wildcard-expand db frame frames eid pattern))))))

(defn- pull-pattern-frame
  [db [frame & frames]]
  (ha/go-try
   (if-let [eids (seq (:eids frame))]
     (if (:wildcard? frame)
       (ha/<? (pull-wildcard db
                             (assoc frame
                                    :specs []
                                    :eid (first eids)
                                    :wildcard? false)
                             frames))
       (if-let [specs (seq (:specs frame))]
         (let [spec       (first specs)
               pattern    (:pattern frame)
               new-frames (conj frames (assoc frame :specs (rest specs)))]
           (ha/<? (pull-attr db spec (first eids) new-frames)))
         (->> frame :kvps persistent! not-empty
              (reset-frame frame (rest eids))
              (conj frames)
              (pull-pattern-frame db)
              (ha/<?))))
     (conj frames (assoc frame :state :done)))))

(defn- pull-pattern
  [db frames]
  (ha/go-try
   (case (:state (first frames))
     :expand     (ha/<? (pull-pattern db (pull-expand-frame db frames)))
     :expand-rev (ha/<? (pull-pattern db (pull-expand-reverse-frame db frames)))
     :pattern    (ha/<? (pull-pattern db (ha/<? (pull-pattern-frame db frames))))
     :recursion  (ha/<? (pull-pattern db (pull-recursion-frame db frames)))
     :done       (let [[f & remaining] frames
                       result (cond-> (persistent! (:results f))
                                (not (:multi? f)) first)]
                   (if (seq remaining)
                     (->> (cond-> (first remaining)
                            result (update :kvps assoc! (:attr f) result))
                          (conj (rest remaining))
                          (pull-pattern db)
                          (ha/<?))
                     result)))))

(defn pull-spec
  [db pattern eids multi?]
  (ha/go-try
   (let [eids (into [] (ha/<? (ha/map< #(ha/go-try
                                         (ha/<? (db/entid-strict db %)))
                                       eids)))]
     (ha/<? (pull-pattern db (list (initial-frame pattern eids multi?)))))))

(defn pull [db selector eid]
  {:pre [(db/db? db)]}
  (ha/go-try
   (ha/<? (pull-spec db (dpp/parse-pull selector) [eid] false))))

(defn pull-many [db selector eids]
  {:pre [(db/db? db)]}
  (ha/go-try
   (ha/<? (pull-spec db (dpp/parse-pull selector) eids true))))
