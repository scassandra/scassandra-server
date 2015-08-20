## Priming Collections

As of version ```0.5.0```

Stubbed Cassandra supports priming maps, lists and sets of all types. Stubbed Cassandra does not support user defined types yet.

Collections can be in returned rows or prepared statement variables. For both you need to define the type.


All the examples assume the following static imports:

```java
import static org.scassandra.http.client.types.ColumnMetadata.*;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.MapType.*;
import static org.scassandra.cql.SetType.*;
import static org.scassandra.cql.ListType.*;

```

### Priming Lists

You need to prime the column type to be of the list type of your schema. For example if you had a column type that is a list of ints like interesting_dates in this example:

```java
Date today = new Date();
Map<String, ?> row = ImmutableMap.of(
        "name", "Chris Batey",
        "age", 29,
        "interesting_dates", Lists.newArrayList(today.getTime())
        );
PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
        .withQuery("select * from person where name = ?")
        .withVariableTypes(VARCHAR)
        .withColumnTypes(column("age", INT), column("interesting_dates", list(TIMESTAMP)))
        .withRows(row)
        .build();
primingClient.prime(preparedStatementPrime);
```

To see what Java type you need to put in your rows map see [ColumnTypes](http://localhost:4000/java-client/column-types/) page.

How to construct the correct CqlType should be intuative e.g list(INT), list(VAR_INT).

If a variable in the prepared statement you need to prime it like this:

```java
PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
        .withQueryPattern(".*person.*")
        .withVariableTypes(VARCHAR, INT, list(TIMESTAMP))
        .build();
primingClient.prime(preparedStatementPrime);

````

Where the prepared statement has three variables (?s), the first one being a varchar, second an int and last a list of timestamps.

### Priming Sets

Same as Lists apart from it is set(INT) etc

### Priming Maps

Maps require two types so you need to give the static method map two arguments e.g. map(TEXT, INT) represents map<text, int>
