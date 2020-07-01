# cos418
## Distributed Systems

[Raft Consensus Algorithm](https://raft.github.io/raft.pdf) CS418 Assignment in Go from 

[Princeton University](https://www.cs.princeton.edu/), [COS-418: Distributed Systems](https://www.cs.princeton.edu/courses/archive/fall16/cos418/), [Mike Freedman](http://www.cs.princeton.edu/~mfreed/) and [Kyle Jamieson](http://www.cs.princeton.edu/~kylej/). Includes [lecture on Raft (PPTX)](https://www.cs.princeton.edu/courses/archive/fall16/cos418/docs/L8-consensus-2.pdf) and programming assignments to build a Raft-based key-value store. (Fall 2016, ...)

See also [Raft Github](https://raft.github.io/) for more.

# Notice
- The code is intentionally jumbled up and there are a lot of unnamed goroutines.

- The src folder also contains MapReduce Golang.

# Interpretation
My interpretation is based on Producer-Consumer concept. 

Incoming requests and heartbeat timers are producers and consumers are logic that work on those products (E.g Sending RPCs, appending logs, ...). There are a lot of channels being used due to this concept. If I were to re-write, I wil use mutexes and global state to synchronise the logic flow.

There are some instances that locks are not used on shared resources because they are segregated by the Leader and Follower state. (Eg. logs).

# Take-away (stuff that were wondered during the work on the assignment)
1. How to implement Leader-Append-only guarantee?. 
2. What is 'prevLog' as mentioned in the paper. Is it the last log of old entries, or the last log of newer entries?
3. How to handle committed logs? If two RPCs are sent, one is for index 2-4 (A), and the other 5-7 (B). If both (A) and (B) are received by the followers and replied, what if reply (B) reaches the Leader first followed by reply(A)?
4. Defining 'majority'.

// More to be added.

