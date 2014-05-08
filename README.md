# Scassandra - Making testing Cassandra easy

WARNING: Scassandra is under constant development and the first version is yet to be released. We will be making breaking changes to the API over the coming months and plan to release a stable version in the summer of 2014.

Scassandra is an open source tool that enables easy unit and integration testing of code that accesses Cassandra. 

It can also be used for acceptance testing of applications that use Cassandra. 

It is especially aimed edge case testing such as read and write timeouts.

It acts as a real Cassandra instance and can be primed to respond with results or with exceptions like read timeouts. It does this by implementing the server side of the CQL binary protocol.

Scassandra is written in Scala but we also have a Java wrapper for use with Java unit and integration tests. Simply add the following maven repo and dependency to your pom or gradle build file.
* Maven repo: https://raw.githubusercontent.com/scassandra/scassandra-repo/master/snapshots
* Deppendency: 'org.scassandra:java-client:0.0-SNAPSHOT'

Please read the list of Gotchas:
* [Gotcahas](https://github.com/scassandra/scassandra-server/wiki/Gotchas)


In addition to a binary port Scassandra opens an admin port for priming and verifying queries. Information on how to prime is here:

* [Priming](https://github.com/scassandra/scassandra-server/wiki/Priming)
* [Activity Verification](https://github.com/scassandra/scassandra-server/wiki/Activity-Verification)
* [Using Scassandra from Java Gradle] (https://github.com/scassandra/scassandra-server/wiki/Using-Scassandra-from-Java-Gradle)

We plan to release a standalone executable (we haven't yet). You can checkout Scassandra and build an executable jar for now:
* [Getting started](https://github.com/scassandra/scassandra-server/wiki/Getting-Started)


Current limitations:
* Only tested with Java Datastax driver version 2 and above. Support for version 1 will come later. Other drivers will be tested and supported soon. 
* No support for applications that use Prepared statements. Support is on its way.
* Collections aren't supported.
* Custom types aren't supported.
* Binary protocol only. No planned support for thrift.

For feature requests and bug reports send an email to: Christopher.batey@gmail.com


