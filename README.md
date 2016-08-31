# Dyanmo-Style-KVStorage

A simplified version of Amazon Dynamo.

There are three main pieces:

1) Partitioning
2) Replication
3) Failure handling

The main goal is to provide both availability and linearizability at the same time. The implementation always performs read and write operations successfully even under failures. At the same time, a read operation always returns the most recent value. SHA-1 hash function is used to generate keys.

The code is meant to work on multiple Android emulators which function as different systems/nodes.

Each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

*Quorum replication*
For linearizability, a quorum-based replication has been implemented. The replication degree is 3. Both the reader quorum size and the writer quorum size is 2.

*Failure handling*
Failure is detected by a timeout for a socket read. Code does not rely on socket creation or connect status to determine if a node has failed.
