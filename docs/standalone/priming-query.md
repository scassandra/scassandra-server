## Priming Queries

The endpoint for priming queries is:

```
POST on http://[host]:[admin-port]/prime-query-single
```

#### Successful response

```json
 {
   "when": {
     "query" :"select * from people"
   },
   "then": {
     "rows" :[{"name":"Chris", "age":"28"}, {"name":"Alexandra", "age":"24"}]
   }
 }
```


#### Specific consistencies

```json
 {
   "when": {
     "query" :"select * from people",
     "consistency" : ["ONE", "TWO"]
   },
   "then": {
     "rows" :[{"name":"Chris", "age":"28"}, {"name":"Alexandra", "age":"24"}]
   }
 }  
```

#### Unsuccessful response:

By default the above priming primes for queries regardless of consistency.

```json
 {
   "when": {
     "query" :"select * from people"
   },
   "then": {
     "result" : "read_request_timeout"
   }
}
```

Where result can be:

```
read_request_timeout
write_request_timeout
unavailable
server_error
protocol_error
bad_credentials
overloaded
is_bootstrapping
truncate_error
syntax_error
unauthorized
invalid
config_error
already_exists
unprepared
closed_connection
```


#### Column types

By default all column types are varchars. To specify a specific column type add the column_types object to your ```then``` object. 

Column types listed below correspond to [CQL types](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cql_data_types_c.html).

##### Priming ascii

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "some ASCII value"}] ,
     "column_types" : { "column1" : "ascii"}
   }
 }
```

##### Priming varchar

Varchars can be primed in two ways.

1 - By not specifying any column type

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "some text"}] 
   }
 }
```

2 - By specifying the varchar column type

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "some text"}],
     "column_types" : { "column1" : "varchar"}
   }
 }
```

##### Priming text

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "some CQL text"}] ,
     "column_types" : { "column1" : "text"}
   }
 }
```

##### Priming int

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "123"}] ,
     "column_types" : { "column1" : "int"}
   }
 }
```

##### Priming bigint

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "1234567899"}] ,
     "column_types" : { "column1" : "bigint"}
   }
 }
```

##### Priming decimal

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "4.3456"}] ,
     "column_types" : { "column1" : "decimal"}
   }
 }
```

##### Priming double

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "4.3456"}] ,
     "column_types" : { "column1" : "double"}
   }
 }
```
##### Priming float

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "4.3456"}] ,
     "column_types" : { "column1" : "float"}
   }
 }
```

##### Priming varint

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "10111213141516171819"}] ,
     "column_types" : { "column1" : "varint"}
   }
 }
```

##### Priming counter

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "1234"}] ,
     "column_types" : { "column1" : "counter"}
   }
 }
```

##### Priming boolean

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "true"}] ,
     "column_types" : { "column1" : "boolean"}
   }
 }
```

##### Priming blob

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "0x48656c6c6f"}] ,
     "column_types" : { "column1" : "blob"}
   }
 }
```

##### Priming timestamp

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "1401049038478"}] ,
     "column_types" : { "column1" : "timestamp"}
   }
 }
```

##### Priming uuid

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "668506f7-7d08-4aeb-a8cd-3b4b22911157"}] ,
     "column_types" : { "column1" : "uuid"}
   }
 }
```
##### Priming time uuid

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "2c530380-b9f9-11e3-850e-338bb2a2e74f"}] ,
     "column_types" : { "column1" : "timeuuid"}
   }
 }
```

##### Priming inet

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": "127.0.0.1"}] ,
     "column_types" : { "column1" : "inet"}
   }
 }
```
##### Priming set

```json
 {
   "when": {
     "query" :"select * from entries"
   },
   "then": {
     "rows" :[{"column1": ["one", "two", "three"]}] ,
     "column_types" : { "column1" : "set<text>"}
   }
 }
```

#### Errors when priming

See the [priming errors page](http://www.scassandra.org/scassandra-server/priming-errors)

#### Seeing your existing primes

```
GET on http://[host]:[admin-port]/prime-query-single
```

This will include a list of all primes you have sent to the server with the defaults filled in.

#### Deleting all query primes

```
DELETE on http://[host]:[admin-port]/prime-query-single
```

