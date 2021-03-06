(ns hitchhiker.tree.node
  #?(:cljs (:refer-clojure :exclude [-lookup]))
  #?(:clj (:refer-clojure :exclude [satisfies?]))
  (:require
   #?(:clj [hitchhiker.tree.utils.platform :refer [satisfies?]])))

(defprotocol IEDNOrderable
  (-order-on-edn-types [t]))

(defprotocol IAddress
  (-dirty? [node] "Returns true if this should be flushed")
  (-dirty! [node] "Marks a node as being dirty if it was clean")
  (-resolve-chan [node] "Returns the INode version of this node; could trigger IO, returns a core.async promise-chan"))

(defprotocol INode
  (-last-key [node] "Returns the rightmost key of the node")
  (-overflow? [node] "Returns true if this node has too many elements")
  (-underflow? [node] "Returns true if this node has too few elements")
  (-merge-node [node other] "Combines this node with the other to form a bigger node. We assume they're siblings")
  (-split-node [node] "Returns a Split object with the 2 nodes that we turned this into")
  (-lookup [node k] "Returns the child node which contains the given key"))

;; marker protocols
(defprotocol IDataNode
  (-foo1 [node] "Does nothing"))
(defprotocol IIndexNode
  (-foo2 [node] "Does nothing"))
(defprotocol IResolved
  (-foo3 [node] "Does nothing"))

(defn address?
  [node]
  (satisfies? IAddress node))
