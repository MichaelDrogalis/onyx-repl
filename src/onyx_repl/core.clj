(ns onyx-repl.core
  (:refer-clojure :exclude [map mapcat filter remove])
  (:require [clj-sockets.core :refer [create-socket write-line close-socket]])
  (:import [java.util UUID]))

(defn invoke-map [f segment]
  {:value (f (:value segment))})

(defn invoke-mapcat [f segment]
  [{:value (f (:value segment))}])

(defn invoke-filter [f segment]
  (if (f (:value segment))
    segment
    []))

(defn invoke-remove [f segment]
  (if (f (:value segment))
    []
    segment))

(defn new-task-name []
  (keyword (str "task-" (UUID/randomUUID))))

(defn define-remote-named-function! [socket task-name f]
  (let [f-body (rest (read-string (pr-str local-inc)))
        f-def (pr-str (conj f-body (symbol (name task-name)) 'defn))]
    (write-line socket f-def)))

(defn define-remote-anonymous-function! [socket task-name f]
  (write-line
   socket
   (format "(def %s %s)"
           (name task-name)
           f)))

(defmacro defn-remote [& body]
  `(defn ~@body))

(defn link-workflow-and-head [job task-name]
  (-> job
      (assoc-in [:head] task-name)
      (update-in [:workflow] conj [(:head job) task-name])))

(defn map-catalog-entry [task-name]
  {:onyx/name task-name
   :onyx/fn ::invoke-map
   :onyx/type :function
   :onyx/batch-size 20
   :onyx-repl/fn (keyword "user" (name task-name))})

(defn mapcat-catalog-entry [task-name]
  {:onyx/name task-name
   :onyx/fn ::invoke-mapcat
   :onyx/type :function
   :onyx/batch-size 20
   :onyx-repl/fn (keyword "user" (name task-name))})

(defn filter-catalog-entry [task-name]
  {:onyx/name task-name
   :onyx/fn ::invoke-filter
   :onyx/type :function
   :onyx/batch-size 20
   :onyx-repl/fn (keyword "user" (name task-name))})

(defn remove-catalog-entry [task-name]
  {:onyx/name task-name
   :onyx/fn ::invoke-remove
   :onyx/type :function
   :onyx/batch-size 20
   :onyx-repl/fn (keyword "user" (name task-name))})

(defmacro repl-function [job f catalog-f]
  `(let [job# ~job
         task-name# (new-task-name)]
     (if (fn? ~f)
       (define-remote-named-function! (:socket job#) task-name# '~f)
       (define-remote-anonymous-function! (:socket job#) task-name# '~f))
     (-> job#
         (link-workflow-and-head task-name#)
         (update-in [:catalog] conj (~catalog-f task-name#)))))

(defmacro map [job f]
  `(repl-function ~job ~f ~map-catalog-entry))

(defmacro mapcat [job f]
  `(repl-function ~job ~f ~mapcat-catalog-entry))

(defmacro filter [job f]
  `(repl-function ~job ~f ~filter-catalog-entry))

(defmacro remove [job f]
  `(repl-function ~job ~f ~remove-catalog-entry))

(defn remote-onyx-connection [host port]
  (create-socket "localhost" 5555))

(defn onyx-input [conn]
  (let [task-name (new-task-name)]
    {:head task-name
     :socket conn
     :workflow []
     :catalog [{:onyx/name task-name
                :onyx/plugin :onyx.plugin.core-async/input
                :onyx/type :input
                :onyx/medium :core.async
                :onyx/batch-size 20
                :onyx/max-peers 1}]}))

(defn explain [job]
  (select-keys
   job
   [:workflow :catalog :flow-conditions
    :lifecycles :windows :triggers]))

(defn-remote foo [x]
  (inc x))

(let [conn (remote-onyx-connection "localhost" 5555)]
  (-> (onyx-input conn)
      (map (fn [x] (* 2 x)))
      (map local-inc)
      (clojure.pprint/pprint)))


(defn-remote peers foo
  (fn [x]
    (inc x)))


(defn-remote hello []
  :bye)


