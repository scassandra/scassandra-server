### Priming Prepared Statements

Endpoint for priming prepared statements:

```
POST on http://[host]:[admin-port]/prime-prepared-single
```
Priming prepared statements is more complex than queries as the types of the variables need to be defined and the types of the columns in the result. If you fail to provide either the variable_types or column_types everything defaults to varchar.

It is currently not possible to prime based on variable values when the prepared statement is executed.

#### Successful response

Here is an example of priming both the types of the variables (the ?s) and the column types in the result.

```json
{
   "when":{
      "query":"insert into people(ascii_column, blob_column, boolean_column, timestamp_column,
uuid_column, varchar_column, timeuuid_column, inet_column) = (?,?,?,?,?,?,?,?,?)"
   },
   "then":{
      "variable_types":[
         "ascii",
         "blob",
         "boolean",
         "timestamp",
         "uuid",
         "varchar",
         "timeuuid",
         "inet"
      ]
   }
}
```

Here is an example of the priming the variable_types and the column_types of the rows that you prime:

```json
{
  "when": {
    "query": "select * from people"
  },
  "then": {
    "variable_types": [
      "varchar"
    ],
    "rows": [
      {
        "name": "Chris"
      }
    ],
    "result": "success",
    "column_types": {
      "name": "varchar"
    }
  }
}
```

Drivers will use the variable_type information to process it when you come to execute your preparaed statement. The Java Driver will throw an exception if what you pass to bind fails to match the types of what Cassandra indicated the ?s types were.

The order if the types in the variable_types array should match the order of the ?s in the query.

#### Unsuccessful response:

Just like queries you can prime unsuccessful responses for prepared statements.

```json
 {
   "when": {
     "query" :"select * from people where name = ?"
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

#### Seeing your existing primes
```
GET on http://[host]:[admin-port]/prime-prepared-single
```

#### Deleting all prepared statement primes
```
DELETE on http://[host]:[admin-port]/prime-prepared-single
```