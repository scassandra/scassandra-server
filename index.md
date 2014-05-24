---
layout: default
---

###Overview
Scassandra is an open source tool that enables easy unit and integration testing of code that accesses Cassandra.

It can also be used for acceptance testing of applications that use Cassandra.

It is especially aimed edge case testing such as read and write timeouts.

It acts as a real Cassandra instance and can be primed to respond with results or with exceptions like read timeouts. It does this by implementing the server side of the CQL binary protocol.

Scassandra is written in Scala but we also have a Java wrapper for use with Java unit and integration tests. If you're looking to test your Cassandra application then go to the [Java Client](www.scassandra.org/java-lcient).

To get started please see the wiki:

* [Wiki](https://github.com/scassandra/scassandra-server/wiki)

###Features v0.1:
* Priming of queries with columns of all the primitive types (no suport for collections/custom tyes).
* Priming of prepared statements. The variable (?s) types and response types can be any of the primitive types.
* Retrieval of a list of all recorded queries.
* Retrieval of a list of all the recorded executed prepared statements. If the prepared statement has been primed then the variable values are also visible.

###Feature backlog:
* Retrieval of a list of all prepared statements even if they haven't been executed.
* Priming of tables rather than queries. Currently Scassandra does not parse the query and compares an executed query with all the primes query field. This would be very useful for priming the system keyspace as certain drivers expect the same thing to be in system.local but do slightly different queries to retireve it.


###Current limitations:
* Only tested with Java Datastax driver version 2 and above. Support for version 1 will come later. Other drivers will be tested and supported soon.
* Collections aren't supported.
* Custom types aren't supported.
* Binary protocol only. No planned support for thrift.

For feature requests and bug reports send an email to: Christopher.batey@gmail.com


