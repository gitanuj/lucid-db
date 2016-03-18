#### Running on Eucalyptus:

* 'lucid.config' should have internal IPs of the servers.

* Start servers:
    all on same instance:
      java -cp lucid-db.jar com.lucid.ycsb.StartServers 0   -> for SpannerDB     [or]
      java -cp lucid-db.jar com.lucid.ycsb.StartServers 1   -> for RCDB
    
    Each datacenter on same instance:
      java -cp lucid-db.jar com.lucid.ycsb.StartDatacenter 0 0   -> for SpannerDB-datacenter 0 
      java -cp lucid-db.jar com.lucid.ycsb.StartDatacenter 0 1   -> for SpannerDB-datacenter 1  [or]
      java -cp lucid-db.jar com.lucid.ycsb.StartDatacenter 1 0  -> for RCDB-datacenter 0
      java -cp lucid-db.jar com.lucid.ycsb.StartDatacenter 1 1  -> for RCDB-datacenter 1

* Start client as:
      java -cp lucid-db.jar com.yahoo.ycsb.Client -t -db com.lucid.ycsb.SpannerDB -P workloads/workloada    [or]
      java -cp lucid-db.jar com.yahoo.ycsb.Client -t -db com.lucid.ycsb.RCDB -P workloads/workloada
