# transport.janet
#
# Author: David Beazley (@dabeaz)
#         https://www.dabeaz.com
#
# Low-level message transport functions.  These send size-prefixed
# messages across a socket.

(defn send-size [size sock]
  (net/write sock (string/format "%10d" size))
)

(defn receive-size [sock]
      (parse (net/chunk sock 10))
)

(defn send-message [msg sock]
  (send-size (length msg) sock)
  (net/write sock msg)
  )

(defn receive-message [sock]
    (net/chunk sock (receive-size sock))
)




