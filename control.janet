# control.janet
#
# Author: David Beazley (@dabeaz)
#         https://www.dabeaz.com
#
# Controller environment for the Raft distributed consensus algorithm.
# See https://raft.github.io.
#
# This file defines the basic elements of a controller for the Raft
# algorithm.  The controller is responsible for actually running
# the core algorithm and any aspect of its networking, timing, etc.
#
# To use this:  Run at the terminal like this:
#
#   bash % janet control.janet <nodenum>
#
# Since it's distributed consensus, you need to run at least two
# other servers in different terminals.  One of them will eventually
# become leader.  You can run up to 5 servers based on the default
# configuration.
#
# On the leader: you can type the following to inject an entry to
# the replicated Log:
#
#   repl:1:> (client-append-entry "hello")
#
# If it's working, you should see the entry applied to logs of all
# other servers.
#
# For debugging the Raft state, you can type (raftdebug) at the REPL.
# This probably won't make sense if you haven't read the Raft paper.
  
(import raft)
(import transport)

# Raft Configuration parameters.  These are greatly slowed down to
# make everything observable.
(def HEARTBEAT_TIMER 1)
(def ELECTION_TIMER_BASE 5)
(def ELECTION_TIMER_JITTER 3)
(def SERVERS @{
	       0 @["127.0.0.1" 15000]
	       1 @["127.0.0.1" 15001]
	       2 @["127.0.0.1" 15002]
	       3 @["127.0.0.1" 15003]
	       4 @["127.0.0.1" 15004]
	       }
  )

# Reference to the controller object created by run_server
(var controller nil)

# Thread that generates a periodic heartbeat event
(defn generate-heartbeats [parent]
  (while true
    (os/sleep HEARTBEAT_TIMER)
    (thread/send parent 'heartbeat)
    )
  )

# Thread that generates periodic election timeout events
(defn generate-election-timeouts [parent]
  (while true
    (os/sleep (+ ELECTION_TIMER_BASE (* ELECTION_TIMER_JITTER (math/random))))
    (thread/send parent 'election-timeout)
    )
  )

(defn decode-message [rawmsg]
  (unmarshal rawmsg)
  )

(defn encode-message [msg]
  (marshal msg)
  )

# Thread that waits for incoming messages
(defn receive-messages [parent]
  (defn handler [conn]
    (while true
      (var msg (transport/receive-message conn))
      (if (nil? msg)
	(break)
	(thread/send parent (decode-message msg))
	)
      )
    )
  (def myself (thread/receive))
  (def [host port] (SERVERS myself))
  (print "Running Server on port " port)
  (net/server "0.0.0.0" port handler)
  )
  
# Thread that sends outgoing messages
(defn send-messages [parent]
  (def peer (thread/receive))
  (def [host port] (SERVERS peer))
  (var sock nil)
  (while true
    (try
      (do
	(var msg (thread/receive 1000))
	(if (nil? sock)
	  (do
	    # (print "Trying to connect to " host port)
	    (set sock (net/connect host port))
	    # (print "Connected " sock)
	    )
	  )
	(if (not (nil? sock))
	  (transport/send-message (encode-message msg) sock)
	  )
	)
      ([err]
       # (print "ERROR:" err)
       (set sock nil)
       )
      )
    )
  )

# Event processor. This dispatches events to the core Raft algorithm.
(defn process-events [serv control]
  # Launch supporting threads
  (thread/send (thread/new receive-messages) (control :address))
  (def channels @{})
  (each peer (control :peers)
    (put channels peer (thread/new send-messages))
    (thread/send (channels peer) peer)
    )
  (thread/new generate-heartbeats)
  (thread/new generate-election-timeouts)

  # Run the server
  (while true
    (var evt (thread/receive 10000))
    (cond (= evt 'heartbeat) (raft/handle-heartbeat serv control)
	  (= evt 'election-timeout) (raft/handle-election-timeout serv control)
	  (raft/handle-message serv evt control)
	  )

    # After processing events, look for outgoing messages
    (each msg (control :outgoing)
      (do
	(thread/send (channels (msg :dest)) msg)
	)
      )
    (put control :outgoing @[])
    )
  )

(defn RaftControl [address peers]
  @{:type 'RaftControl
    :address address
    :peers peers
    :outgoing @[]
    
    # --- State Machine methods. These are executed by the Raft algorithm
    # --- as a callback.  They can be customized if you want to make a
    # --- plugable execution environment.
    
    :send
      (fn [self msg]
	(array/concat (self :outgoing) msg)
	)

    :apply-state-machine
      (fn [self entries]
	(print (self :address) " Applying:")
	(pp entries)
	)
   })

# Helper functions to interact with servers at the REPL
(defn client-append-entry [item &opt control]
  (def msg (raft/ClientAppendEntry item))
  (if (nil? control)
    (thread/send controller msg)
    (thread/send control msg)
    )
  )

(defn raftdebug []
  (thread/send controller (raft/RaftDebug))
  )

# Start a new Raft server in a separate thread.  Returns the thread-id should you
# want to send it messages

(defn run-server [address]
  (defn run [ parent ]
    (process-events (raft/ServerState) (RaftControl address (array/remove (range (length SERVERS)) address)))
    )
  (set controller (thread/new run))
  )


# Enclosing definition environment for use below
(def main-env (fiber/getenv (fiber/current)))

(defn main [name nodenum]
  (run-server (parse nodenum))
  (repl nil nil main-env)
)	   

