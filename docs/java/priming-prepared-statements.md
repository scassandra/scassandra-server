## Priming Prepared Statements

You need to prime any prepared statements your application is going to use before your application prepares the statement,
not just before they are executed. If your application prepares them on startup you should prime Scassandra before you
start your application.

This is because when your application prepares the statement Cassandra responds with the types of each of the variable's (?s)
type information. Drivers use this information to validate your execution of the prepared statement without sending it to
Cassandra. So if you do not prime before your application prepares the statement Scassandra can only guess what the types are,
 and it defaults them all to varchar.

You build priming requests with the PrimingRequestBuilder. The builder takes the following:

* **Query** - The only mandatory property e.g "select * from person where name = ?"
* **Consistency** - Defaults to all consistencies. Can override to ONE, TWO, LOCAL_QUORUM etc
* **Result** - Defaults to Success. Other values are Read timeout exception, Write timeout exception and Unavailable exception
* **ColumnTypes** - All columns default to Varchar. You'll need to override this.
* **VariableTypes** - This is the type of each of the? in the prepared statement. All variable types default to Varchar. You'll need to override this.


#### Priming variable types, without any rows

This primes a prepared statement with two variables, where the first is a varchar and the second is an int. You specify
the variable types in the order they appear in the prepared statement text.

Assuming you have the following static imports:

```java
import static org.scassandra.http.client.types.ColumnMetadata.*;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.MapType.*;
import static org.scassandra.cql.SetType.*;
import static org.scassandra.cql.ListType.*;
import static org.scassandra.http.client.MultiPrimeRequest.*;

```
Prime a prepared statement:

```java
PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
    .withQuery("insert into person(first_name, age) values (?,?)")
    .withVariableTypes(VARCHAR, INT)
    .build();
primingClient.primePreparedStatement(preparedStatementPrime);
```

As this is an insert then you don't need to specify any rows to return.

#### Priming variable types and rows with column types

You want to prime the variable types of the prepared statement as well as
returned rows with column types other than varchar.

```java
Map<String, ? extends  Object> row = ImmutableMap.of(
        "first_name", "Chris",
        "last_name", "Batey",
        "age", 29);
PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
        .withQuery("select * from person where first_name = ?")
        .withVariableTypes(VARCHAR)
        .withColumnTypes(column("age", INT)
        .withRows(row)
        .build();
primingClient.primePreparedStatement(preparedStatementPrime);
```

#### Priming based on bound variables

Sometimes the need arises to prime based on the actual variables bound during execution. For example imagine the following
prepared statement:

```cql
select * from events where name = ?
```

And you want to have a successful result of the bound variable is 'Chris' but a read time out if the bound variable is
'Andrew'. You can achieve this with a multi prime:

```
MultiPrimeRequest prime = MultiPrimeRequest.request()
        .withWhen(when()
                .withQuery("select * from events where name = ?"))
        .withThen(then()
                .withVariableTypes(TEXT)
                .withOutcomes(
                        outcome(match().withVariableMatchers(variableMatch().withMatcher("Chris").build()), action().withResult(Result.success)),
                        outcome(match().withVariableMatchers(variableMatch().withMatcher("Andrew").build()), action().withResult(Result.read_request_timeout))
                )
        )
        .build();

primingClient.multiPrime(prime);

```

For variables you do not want to match on you can use the ```anyMatch()```

Primes are identified by the `queryText` and `consistency` so you can't have two primes with the same `queryText` and `consistency` but with different
outcomes as they will overwrite each other.

If none of the outcomes match then a RuntimeException will currently be thrown. This will be replaced with default behaviour of returning an empty successful result.

Collections are not currently supported for variable matching.

#### Priming errors

Works the same as with queries. See [priming queries]({{ site.baseurl }}/priming-query)

#### Verifying prepared statement execution

See the [Hamcrest Matchers]({{ site.baseurl }}/hamcrest-matchers) page.

#### Resetting your primes

No need to restart Scassandra. You can either remove all your prepared statement primes or all primes (including prepared statement primes).

```java
primingClient.clearAllPrimes();
primingClient.clearPreparedPrimes();

```