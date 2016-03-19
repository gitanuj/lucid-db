#Lucid DB

Implementation of Spanner and Replicated Commit

##Config
'lucid.config' is the configuration file which contains the following:

1. IP address of all the shards (with distinct paxos, server and client ports)

2. Number of datacenters

3. Timeouts and Latencies

Each shard is fully replicated. All the replicas of a shard are assumed to be listed together. Order of shards is same for all datacenters. For eg. X1, X2, Y2, Y2 where 1 & 2 are datacenters and X & Y are shards.

Refer to `lucid-rc.config` and `lucid-spanner.config` for examples.

The implementation assumes that `lucid.config` contains valid information at all times.

## Running on Eucalyptus:

* 'lucid.config' should have internal IPs of the servers.

* Start servers:
    all on same instance:
      ```
      java -cp lucid-db.jar com.lucid.ycsb.StartServers 0   -> for SpannerDB     [or]
      java -cp lucid-db.jar com.lucid.ycsb.StartServers 1   -> for RCDB
      ```
    
    Each datacenter on same instance:
      ```
      java -cp lucid-db.jar com.lucid.ycsb.StartDatacenter 0 0   -> for SpannerDB-datacenter 0 
      java -cp lucid-db.jar com.lucid.ycsb.StartDatacenter 0 1   -> for SpannerDB-datacenter 1  [or]
      java -cp lucid-db.jar com.lucid.ycsb.StartDatacenter 1 0  -> for RCDB-datacenter 0
      java -cp lucid-db.jar com.lucid.ycsb.StartDatacenter 1 1  -> for RCDB-datacenter 1
      ```

* Start client as:
      ```
      java -cp lucid-db.jar com.yahoo.ycsb.Client -t -db com.lucid.ycsb.SpannerDB -P workloads/readyheavy1k    [or]
      java -cp lucid-db.jar com.yahoo.ycsb.Client -t -db com.lucid.ycsb.RCDB -P workloads/readyheavy1k
      ```
