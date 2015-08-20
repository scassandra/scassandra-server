## Recorded Activity

If you want to verify your application executes the correct queries rather than testing behaviour via stubbing you can
retrieve all the queries, connections and prepared statements that have been executed.

This can be useful if you want to verify queries are executed at the correct consistency, or are retried the correct
number of times.

#### Queries

```java
List<Query> queries = activityClient.retrieveQueries();

// Where each Query has:
.getQuery()
.getConsistency()
```

#### Prepared Statement Preparations

```java
List<PreparedStatementPreparation> preparations = activityClient.retrievePreparedStatementPreparations();

// Where each PreparedStatementPreparation has:
.getPreparedStatementText()

```

#### Prepared Statement Executions

```java
List<PreparedStatementExecution> executions = activityClient.retrievePreparedStatementExecutions();

// Where each PreparedStatementExecution has:
.getPreparedStatementText()
.getConsistency()
.getVariables()

```

#### Batch Executions

```java
List<BatchExecution> batches = activityClient.retrieveBatches();

// Where each BatchExecution has:
.getBatchStatements()
.getConsistency()
.getBatchType() // UNLOGGED, LOGGED, COUNTER
```

#### Deleting recorded activity

You can reset the recorded activity between tests:

```java
activityClient.clearAllRecordedActivity();
```
