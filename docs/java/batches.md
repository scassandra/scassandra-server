### Batches

As of version ```0.1.0``` you can prime batches based on the queries they contain, the consistency and the batch type (LOGGED, UNLOGGED, COUNTER)

Useful imports:

```java
import static org.scassandra.http.client.BatchType.*;
import static org.scassandra.http.client.BatchQueryKind.*;
import static org.scassandra.http.client.PrimingRequest.*;

```

Priming a batch with just queries for a read request timeout:

```java
primingClient.primeBatch(
                BatchPrimingRequest.batchPrimingRequest()
                        .withQueries(
                                batchQueryPrime("insert something else", query)
                        .withThen(then().withResult(read_request_timeout))

        );
```

For batches that contain prepared statements you must prime the prepared statement first e.g:

```java
primingClient.prime(PrimingRequest.preparedStatementBuilder()
                        .withQuery("insert ? ?")
                        .withThen(then().withVariableTypes(ASCII, INT))
        );
        primingClient.primeBatch(
                BatchPrimingRequest.batchPrimingRequest()
                        .withQueries(
                                batchQueryPrime("insert something else", query),
                                batchQueryPrime("insert ? ?", prepared_statement))
                        .withThen(then().withResult(read_request_timeout))

        );
```

To prime batches other than LOGGED you can override the batch type:


```java
        primingClient.primeBatch(BatchPrimingRequest.batchPrimingRequest()
                        .withConsistency(ONE)
                        .withType(BatchType.COUNTER)
                        .withQueries(BatchQueryPrime.batchQueryPrime(query, BatchQueryKind.query))
                        .withThen(
                                then().withResult(PrimingRequest.Result.read_request_timeout))
        );

```