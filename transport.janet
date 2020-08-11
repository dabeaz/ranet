# transport.janet
#
# Author: David Beazley (@dabeaz)
#         https://www.dabeaz.com
#
# Low-level message transport functions.  These send size-prefixed
# messages across a socket.

(defn send-size [size sock]
  (def err (net/write sock (string/format "%10d" size)))
  (if (not (nil? err))
    (error err))
)

(defn receive-size [sock]
      (parse (net/chunk sock 10))
)

(defn send-message [msg sock]
  (send-size (length msg) sock)
  (def err (net/write sock msg))
  (if (not (nil? err))
    (error err))
)

(defn receive-message [sock]
    (net/chunk sock (receive-size sock))
)




