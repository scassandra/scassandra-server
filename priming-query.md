---
layout: default
---
## Priming Queries

Endpoint for priming queries:

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
```


#### Column types

By default all column types are varchars. To specify a specific column type add the column_types object to your then object.

```json
 {
   "when": {
     "query" :"select * from people"
   },
   "then": {
     "rows" :[{"name":"Chris", "age":28}, {"name":"Alexandra", "age":24}] ,
     "column_types" : { "name" : "varchar" , "age": "int"}
   }
 }
```

Where the types can be [CQL types](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cql_data_types_c.html).

Priming via Java API:

* ascii
* bigint
* blob
* boolean
* counter
* decimal
* double
* float
* int
* timestamp
* uuid
* varchar
* varint
* timeuuid
* inet

#### Errors when priming

When priming for the same query with different consistencies be sure that two different primes do not overlap. E.g One prime for query X for consistencies ONE and TWO and another for the same query X with consistency TWO. This will result in the following error message and a HTTP 405.

~~~ json
{ "errorMessage":"Conflicting Primes", "existingPrimes": ...}
~~~

#### Seeing your existing primes

```
GET on http://[host]:[admin-port]/prime-query-single
```

This will include a list of all primes you have sent to the server with the defaults filled in.

#### Deleting all query primes

```
DELETE on http://[host]:[admin-port]/prime-query-single
```

