## Using Hamcrest matchers to verify interactions with Scassandra

If you want to verify things like Consistency, or that the correct variables were bound in a prepared statement then you
need to retrieve the recorded activity from the Activity Client. Details on the activity client can be found [here]({{ site.baseurl }}/activity)

However working directly with the Query and PreparedStatementExecution objects can be tedious as a lot of type information is lost
when sending the recorded activity via JSON.

That's why we have a Hamcrest matcher to do it for you, with the necessary casting etc all hidden away,

Let's see how we verify a prepared statement is executed at a particular consistency with the correct variables bound.

First off import the matchers:

```java
import static org.scassandra.matchers.Matchers.*;
```

Now lets say you've primed the following prepared statement:

```java
String query = "select * from blah where condition = ?";
PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
    .withQuery(query)
    .withVariableTypes(BOOLEAN)
    .build();
primingClient.prime(primingRequest);
```

And you want to verify that it was called you can now do this:

```java
PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
    .withPreparedStatementText(query)
    .withConsistency("QUORUM")
    .withVariables(true)
    .build();
assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
```

So first we just build up our expected execution then just pass it to the matcher.

To see which Java types you should use for each CQL type see [here]({{ site.baseurl }}/column-types)

Often Strings are used so we don't lose floating point precision.


