## Priming Queries

You build priming requests with the PrimingRequestBuilder. The builder takes the following:

 * **Query** - The only mandatory property e.g "select * from person"
 * **Consistency** - Defaults to all consistencies. Can override to ONE, TWO, LOCAL_QUORUM etc
 * **Result** - Defaults to Success. Other values are Read timeout exception, Write timeout exception and Unavailable exception
 * **ColumnTypes** - All columns default to Varchar. You'll need to override this.

#### Basic Prime - all columns varchar

Priming to return a single row for the query "select * from person" two columns, first\_name and last\_name.

```java
Map<String, String> row = ImmutableMap.of(
        "first_name", "Chris",
        "last_name", "Batey");
PrimingRequest singleRowPrime = PrimingRequest.queryBuilder()
        .withQuery("select * from person")
        .withRows(row)
        .build();
primingClient.primeQuery(singleRowPrime);

```

The type of values in the rows map depend on the type of the column you're stubbing.

#### Prime columns other than varchar

Note this changed a lot in version 0.6.0. Previous versions used the ColumnTypes enum.

First off add some static imports:

```java
import static org.scassandra.http.client.types.ColumnMetadata.*;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.MapType.*;
import static org.scassandra.cql.SetType.*;
import static org.scassandra.cql.ListType.*;

```

This will bring into scope methods for constructing type metadata.

Here is how you would prime a int column. Any columns that are varchar you do not need to supply ColumnMetadata for.
When Cassandra returns rows to a driver it specifies the type. Most drivers will throw an exception if you
request a column with the incorrect type.

```java
Map<String, ?> row = ImmutableMap.of(
        "first_name", "Chris",
        "last_name", "Batey",
        "age", 29);
primingClient.prime(PrimingRequest.queryBuilder()
        .withQuery("select * from person")
        .withColumnTypes(column("age", PrimitiveType.INT))
        .withRows(row)
        .build());

```

Full details of what Java type you should use for each CQL type is [here]({{ site.baseurl }}/column-types).

#### Priming errors

If you want to replicate Cassandra errors you need to override the Result on your prime:

```java
PrimingRequest primeReadRequestTimeout = PrimingRequest.queryBuilder()
        .withQuery("select * from person")
        .withResult(PrimingRequest.Result.read_request_timeout)
        .build();
primingClient.primeQuery(primeReadRequestTimeout);
```

The Result enum contains read request timeout, write request timeout and unavailable. You don't need rows or
column types for this as no rows are returned.

##### Specific errors

If you have retry policies dependent on the number of replicas that responded you may want to have more control over read and write timeouts.

You can do this with an extra parameter to your prime, e.g

```java
WriteTimeoutConfig writeTimeoutConfig = new WriteTimeoutConfig(BATCH_LOG, 2, 3);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(write_request_timeout)
                .withConfig(writeTimeoutConfig)
                .build();
```

This will set the WriteType to BATCH_LOG, received responses to 2 and required responses to 3.

#### Resetting your primes

No need to restart Scassandra. You can either remove all your query primes or all primes (including prepared statement primes).

```java
primingClient.clearQueryPrimes();
primingClient.clearAllPrimes();

```
