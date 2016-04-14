## Priming cluster name

If your application verifies the cluster name on connection you'll need to prime the cluster name.
The Java Datastax Driver looks in the system.local table for the cluster\_name. This method has been tested with the Java Datastax drivers version 1 and 2. If you're using a different driver and want to see how it discovers the cluster_name so you can mock it you can see the queries it makes on startup by getting the recorded queries from Scassandra.


#### Java Datastax Version 2.*


```json
{
  "when": {
    "query": "SELECT * FROM system.local WHERE key='local'"
  },
  "then": {
    "rows": [
      {
        "cluster_name": "custom cluster name",
        "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
        "data_center": "dc1",
        "rack": "rc1",
        "tokens": [
          "1743244960790844724"
        ],
        "release_version": "2.0.1"
      }
    ],
    "result": "success",
    "column_types": {
      "tokens": "set<text>"
    }
  }
}

```

####Java Datastax Driver Version 1.*


The 1.* driver executes a different query;


```json
{
  "when": {
    "query": "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'"
  },
  "then": {
    "rows": [
      {
        "cluster_name": "custom cluster name",
        "rack": "rc1",
        "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
        "data_center": "dc1",
        "tokens": [
          "1743244960790844724"
        ]
      }
    ],
    "result": "success",
    "column_types": {
      "tokens": "set<text>"
    }
  }
}
```

The reason you need to prime more than just the cluster\_name is that the Java Driver expects these fields to be there if there is a row in the system.local table and throws an IllegalArgumentException is they are missing.

To get the value of cluster with the Java driver you call the following on your cluster:


```java
cluster.getMetadata().getClusterName()
```

Don't be fooled by the getClusterName on the cluster object. That is the name you built the object with.

If you're using the Scassandra java client you'll need something like:


```java
 Map<String, ColumnTypes> columnTypes = ImmutableMap.of("tokens",SetType.set(PrimitiveType.TEXT));
        String query = "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'";
        Map<String, Object> row = new HashMap<>();
        row.put("cluster_name", CUSTOM_CLUSTER_NAME);
        row.put("partitioner","org.apache.cassandra.dht.Murmur3Partitioner");
        row.put("data_center","dc1");
        row.put("tokens", Sets.newHashSet("1743244960790844724"));
        row.put("rack","rc1");
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(row)
                .build();
        primingClient.primeQuery(prime);
```
