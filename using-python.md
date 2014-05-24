---
layout: default
---

## Using the Python driver

The Python driver expects certain data to be in the system keyspace and fails if it isn't there. If the queries the Python driver executes changes these primes will stop working.

```
SELECT peer, data_center, rack, tokens, rpc_address, schema_version FROM system.peers
```

-- Empty rows appears to work. No priming required.

Query ran by the Python client:


```
SELECT cluster_name, data_center, rack, tokens, partitioner, schema_version FROM system.local WHERE key='local'
```

Requires:

* cluster_name
* data_center
* rack
* partitioner
* tokens

Example prime:


```json
{
  "when": {
    "query": "SELECT cluster_name, data_center, rack, tokens, partitioner, schema_version FROM system.local WHERE key='local'",
    "consistency": ["ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "SERIAL", "LOCAL_SERIAL", "LOCAL_ONE"],
    "keyspace": "",
    "table": ""
  },
  "then": {
    "rows": [{
      "cluster_name": "custom cluster name",
      "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
      "data_center": "dc1",
      "rack": "rc1",
      "tokens": [
          "1743244960790844724"
        ],
      "release_version": "2.0.1"
    }],
    "result": "success",
    "column_types": {
      "cluster_name": "varchar",
      "partitioner": "varchar",
      "data_center": "varchar",
      "rack": "varchar",
      "tokens": "set",
      "release_version": "varchar"
    }
  }
}
```


Query ran by the current Python driver:


```
SELECT schema_version FROM system.local WHERE key='local'
```

Example prime:


```json
{
  "when": {
    "query": "SELECT schema_version FROM system.local WHERE key='local'"
  },
  "then": {
    "rows": [
      {
        "schema_version": "83266003-03da-3a05-a71f-16193c5267f7"
      }
    ],
    "result": "success",
    "column_types": {
      "tokens": "uuid"
    }
  }
}
```