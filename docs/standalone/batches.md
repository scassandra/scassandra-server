### Batches

As of version ```0.1.0``` you can prime batches based on the queries they contain, the consistency and the batch type where the default is 
LOGGED but can also be set to UNLOGGED and COUNTER.

The endpoint for priming queries is:

```
POST on http://[host]:[admin-port]/prime-batch-single
```

Priming a batch with just queries for a read request timeout:

```json
{
    "when":{
        "queries":[{"text":"any query","kind":"query"}],
        "batchType":"LOGGED"},
    "then": {
        "result":"read_request_timeout"
    }
} 
```
Where batchType can be: ```LOGGED```, ```UNLOGGED``` or ```COUNTER```.

For batches that contain prepared statements you must prime the prepared statement first and then prime e.g.

```json
{
    "when": {
        "queries":[{"text":"Prepared statement that has already been primed","kind":"prepared_statement"}],
        "batchType":"LOGGED"},
    "then": {
        "rows":[], "result":"success"
    }
} 
```
