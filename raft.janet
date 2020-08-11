# raft.janet
#
# Author: David Beazley (@dabeaz)
#         https://www.dabeaz.com
#
# A basic implementation of the Raft distributed consensus protocol.
#
#     https://raft.github.io
#
# This file contains the core elements of the algorithm, but
# none of the associated runtime environment (networks, threads, etc.).
# All runtime elements are found in the controller object.
# See "control.janet" for those details.
#
# This is purely for educational purposes.  It does not implement more
# advanced parts of Raft including cluster reconfiguration or
# snapshots.

# -- Log Entries

(defn LogEntry [term item]
  {:term term
   :item item}
  )

(defn append-entries [log prev_index prev_term entries]
  (cond (>= prev_index (length log)) false
        (< prev_index 0) (do
			   (array/remove log 0 (length log)) 
			   (array/concat log entries) true)
        (not (= ((get log prev_index) :term) prev_term)) false
	(do 
	  (array/remove log (+ 1 prev_index) (length log))
	  (array/concat log entries)
	  true)
	)
  )

(defn test-log []
  (def log @[])
  
  # Appending to an empty log should always work
  (assert (append-entries log -1 0 @[@{:term 1 :item "x"}]))
  
  # Successful append to non-empty log
  (assert (append-entries log 0 1 @[@{:term 1 :item "y"}]))
  
  # Duplicate append
  (assert (append-entries log 0 1 @[@{:term 1 :item "y"}]))
  (assert (= (length log) 2))
  
  # Append would create a hole.  This should fail
  (assert (not (append-entries log 10 1 @[@{:term 1 :item "z"}])))
  
  # Append that fails log-matching property
  (assert (not (append-entries log 1 0 @[@{:term 1 :item "z"}])))
  
  # Empty append
  (assert (append-entries log 1 1 @[]))
  
  # Reset the first entry
  (assert (append-entries log -1 -1 @[@{:term 2 :item "a"}]))
  (assert (= (length log) 1))
  )

# Make sure the basic log features are working
(test-log)

# -- Internal Messages (only used between threads on the same server)

(defn ClientAppendEntry [item]
  {:type 'ClientAppendEntry
   :item item
  }
  )

(defn RaftDebug []
  {:type 'RaftDebug}
  )

# -- Network Messages (sent between servers)

(defn AppendEntries [source dest term prev_index prev_term entries commit_index]
  {:type 'AppendEntries
   :source source
   :dest dest
   :term term
   :prev_index prev_index
   :prev_term prev_term
   :entries entries
   :commit_index commit_index
  }
  )

(defn AppendEntriesResponse [source dest term success match_index]
  {:type 'AppendEntriesResponse
   :source source
   :dest dest
   :term term
   :success success
   :match_index match_index
  }
  )

(defn RequestVote [source dest term last_log_index last_log_term]
  {:type 'RequestVote
   :source source
   :dest dest
   :term term
   :last_log_index last_log_index
   :last_log_term last_log_term
  }
  )

(defn RequestVoteResponse [source dest term vote_granted]
  {:type 'RequestVoteResponse
   :source source
   :dest dest
   :term term
   :vote_granted vote_granted
  }
  )

# -- Server state.

(defn ServerState []
  @{:type 'ServerState
    :state 'FOLLOWER
    :log @[]
    :current_term 0
    :commit_index -1
    :last_applied -1
    :next_index nil
    :match_index nil
    :voted_for nil
    :votes_granted @{}
    :heard_from_leader false
   }
  )

# -- Helper functions

(defn send-one-append-entries [serv node control]
  "Send a single AppendEntries message to a specified node"
  (def prev_index (- (get (serv :next_index) node) 1))
  (def prev_term (if (>= prev_index 0)
		   (((serv :log) prev_index) :term)
		   -1))
  (def entries (array/slice (serv :log) ((serv :next_index) node)))
  (:send control (AppendEntries
		  (control :address)
		  node
		  (serv :current_term)
		  prev_index
		  prev_term
		  entries
		  (serv :commit_index)
		  )
	 )
  )

(defn send-all-append-entries [serv control]
  "Send an AppendEntries message to all of the peers."
  (each peer (control :peers)
    (send-one-append-entries serv peer control)
    )
  )

(defn apply-state-machine [serv control]
  "Apply the state machine once consensus has been reached."
  (if (> (serv :commit_index) (serv :last_applied))
    (do
      (:apply-state-machine control
			    (array/slice (serv :log)
					 (+ (serv :last_applied) 1)
					 (+ (serv :commit_index) 1)
					 )
			    )
      (put serv :last_applied (serv :commit_index))
      )
    )
  )

# -- State transitions

(defn become-leader [serv control]
  "Become the leader"
  (print (control :address) " BECAME LEADER")
  (put serv :state 'LEADER)
  (put serv :next_index (array/new-filled (+ 1 (length (control :peers)))
					  (length (serv :log))))
  (put serv :match_index (array/new-filled (+ 1 (length (control :peers))) -1))
  (send-all-append-entries serv control)
  )

(defn become-follower [serv control]
  "Become a follower"
  (print (control :address) " BECAME FOLLOWER")  
  (put serv :state 'FOLLOWER)
  (put serv :voted_for nil)
  )

(defn become-candidate [serv control]
  "Become a candidate"
  (print (control :address) " BECAME CANDIDATE")    
  (put serv :state 'CANDIDATE)
  (put serv :current_term (+ (serv :current_term) 1))
  (put serv :voted_for (control :address))
  (put serv :votes_granted @{})
  
  (def last_log_index (- (length (serv :log)) 1))
  (def last_log_term (if (>= last_log_index 0)
		       (((serv :log) last_log_index) :term)
		       -1))
  (each peer (control :peers)
    (:send control (RequestVote
		    (control :address)
		    peer
		    (serv :current_term)
		    last_log_index
		    last_log_term)))
  )

# -- Event Handlers.  These are executed by the controller
    
(defn handle-heartbeat [serv control]
  "Leader heartbeat. Occurs on periodic timer to update followers"
  (if (= (serv :state) 'LEADER)
    (send-all-append-entries serv control)
      false)
  )
  
(defn handle-election-timeout [serv control]
  "Called when nothing has been heard from the leader in awhile"
  (if (not (= (serv :state) 'LEADER))
    (if (serv :heard_from_leader)
      (put serv :heard_from_leader false)
      (become-candidate serv control)
    )
    )
  )

(defn handle-message [serv msg control]
  "Top level function for handling a message."

  # Helper function to check message term numbers and whether or not
  # we need to drop into Follower state.
  (defn check-terms [] 
    (if (> (msg :term) (serv :current_term))
      (do
	(put serv :current_term (msg :term))
	(become-follower serv control)
	)
      )
    )
  
  # AppendEntries message received from the leader  
  (defn handle-append-entries []
    (if (= (serv :state) 'CANDIDATE)
      (put serv :state 'FOLLOWER))
  
    (if (= (serv :state) 'FOLLOWER)
      (do
	(def success (append-entries
		      (serv :log)
		      (msg :prev_index)
		      (msg :prev_term)
		      (msg :entries)))
	(def resp (AppendEntriesResponse
		   (msg :dest)
		   (msg :source)
		   (serv :current_term)
		   success
		   (+ (msg :prev_index) (length (msg :entries)))))
	(if (> (msg :commit_index) (serv :commit_index))
	  (do
	    (put serv :commit_index (min (msg :commit_index) (- (length (serv :log)) 1)))
	    (apply-state-machine serv control)
	    )
	  )
	(put serv :heard_from_leader true)
	(:send control resp)
	)
      )
    )

  # AppendEntriesResponse message received from a follower
  (defn handle-append-entries-response []
    (if (= (serv :state) 'LEADER)
      (if (msg :success)
	(do
	  (put (serv :next_index) (msg :source) (+ (msg :match_index) 1))
	  (put (serv :match_index) (msg :source) (msg :match_index))
	  
	  (def match_indices (sorted (serv :match_index)))
	  (array/remove match_indices (control :address))
	  (def commit_index (match_indices (/ (length match_indices) 2)))
	  (if (and (> commit_index (serv :commit_index))
		   (= (((serv :log) commit_index) :term) (serv :current_term)))
	    (do
	      (put serv :commit_index commit_index)
	      (apply-state-machine serv control)	    
	      )
	    )
	  )
	# Unsuccessful operation.  Need to retry with earlier log messages
	(do
	  (put (serv :next_index) (msg :source) (- ((serv :next_index) (msg :source)) 1))
	  (send-one-append-entries serv (msg :source) control)
	  )
	)
      )
    )

  # RequestVote Message received from a Candidate
  (defn handle-request-vote []
    (var success true)
    (if (not (nil? (serv :voted_for)))
      (set success (= (serv :voted_for) (msg :source)))
      )
    # Granting of a vote requires very careful reading of section 5.4.1 of the Raft
    # paper.  We only grant a vote if the candidate's log is at least as up-to-date
    # as our own.
    (def my_last_log_index (- (length (serv :log)) 1))
    (def my_last_log_term (if (>= my_last_log_index 0)
			    (((serv :log) my_last_log_index) :term)
			    -1))
    (if (or (> my_last_log_term (msg :last_log_term))
	    (and (= my_last_log_term (msg :last_log_term))
		 (> my_last_log_index (msg :last_log_index))))
      (set success false)
      )

    (if success (put serv :voted_for (msg :source)))
    
    (:send control (RequestVoteResponse
		    (msg :dest)
		    (msg :source)
		    (serv :current_term)
		    success
		    )
	   )
    )

  # RequestVoteResponse message from a follower
  (defn handle-request-vote-response []
    (if (msg :vote_granted)
      (do
	(put (serv :votes_granted) (msg :source) 1)
	(if (>= (length (serv :votes_granted))
		(/ (length (control :peers)) 2))
	  (do
	    (become-leader serv control)
	    )
	  )
	)
      )
    )


  (defn client-append-entry []
    "Append a new entry to the log as the Raft client."
    (assert (= (serv :state) 'LEADER))
    (def entry (LogEntry (serv :current_term) (msg :item)))
    (def prev_index (- (length (serv :log)) 1))
    (def prev_term (if (>= prev_index 0)
		     (((serv :log) prev_index) :term)
		     -1))
    (append-entries (serv :log) prev_index prev_term @[ entry ])
    )


  (defn debug []
    "Debugging function to output a value from the internal raft state"
    (print "DEBUG:::")
    (pp serv)
    )
  
  # --- Message handling

  (cond
    # -- Internal messages
    (= (msg :type) 'ClientAppendEntry) (client-append-entry)
    (= (msg :type) 'RaftDebug) (debug)
    (do
      (check-terms)
      # -- Network messages
      (if (>= (msg :term) (serv :current_term))
	(cond (= (msg :type) 'AppendEntries) (handle-append-entries)
	      (= (msg :type) 'AppendEntriesResponse) (handle-append-entries-response)
	      (= (msg :type) 'RequestVote) (handle-request-vote)
	      (= (msg :type) 'RequestVoteResponse) (handle-request-vote-response)
	      (error "Unsupported message")
	      )
	)
      )
    )
  )
