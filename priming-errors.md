---
layout: default
---
### Errors when you Prime

If you make a mistake when priming here are some of the errors Scassandra will produce.

#### Conflicting primes

When priming for the same query with different consistencies be sure that two different primes do not overlap. E.g One prime for query X for consistencies ONE and TWO and another for the same query X with consistency TWO. This will result in the following error message and a HTTP 405.

```json
{ "errorMessage":"Conflicting Primes", "existingPrimes": ...}
```

#### Type mismatch

If there is discrepancy between any row values and its column type, scassandra-server responds with ``Bad Request``. The response body contains all detected type mismatches.

Example request:

```json
{
   "when": {
     "query" :"select * from fruits"
   },
   "then": {
     "rows" :[
            {"name":"Orange", "weight_kg": "so light!", "due_date": "1399931804"},
            {"name":"Watermelon", "weight_kg":"5", "due_date": "last week"}
     ],
     "column_types" : { "name" : "varchar" , "weight_kg": "int", "due_date": "timestamp"}
   }
 }
```

Corresponding response:

```json
{
  "typeMismatches": [{
    "value": "so light!",
    "name": "weight_kg",
    "columnType": "int"
  }, {
    "value": "last week",
    "name": "due_date",
    "columnType": "timestamp"
  }]
}
```
