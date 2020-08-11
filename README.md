# ranet

* Author : David Beazley (@dabeaz)
* http://www.dabeaz.com

This is a partial implementation of the Raft distributed consensus
algorithm (https://raft.github.io) in Janet
(https://janet-lang.org). It is purely for educational purposes and my
own amusement.  It is also my first non-trivial Janet program. Use at
your own risk.

Caution:  This code will not work unless you're working from the
latest master branch of the Janet GitHub repo.

## How to use

First, read the Raft paper to understand what's going on.  The goal
of the algorithm is to maintain a distributed replicated transaction
log.  To see this, you'll need to launch 3-5 separate
terminal windows.  In each window, type the following command:

```
bash % janet control.janet <node>
```

Where `<node>` is a number from 0-4 indicating the server number.
You should see output messages such as "BECAME FOLLOWER",
"BECAME CANDIDATE", or "BECAME LEADER" being printed in the
various terminal windows.  If you have at least 3 servers running,
one (and only one) of the sessions will be elected leader.
Go to that terminal window and type a command like this:

```
repl:1:> (client-append-entry "hello")
```

You should see output such as the following appear across
all servers as the log is replicated:

```
n Applying:
@[@{:item "hello" :term 4}]
```

Now, start playing around.  In theory, you can kill any server
(including the leader) and restart it.  It will rejoin the
cluster and have its log restored.   As long as at least 3
servers are running, they will elect a leader.

That's about it.  A lot of Raft functionality is missing (log
persistence, snapshots, membership changes, etc.).  However,
the main purpose of this was to learn more about Janet.
If you look at the code, you'll find all sorts of things with
threads, networking, objects, and more.

I'm open to any suggestions to help me improve the code and
my Janet programming style.

-Dave





