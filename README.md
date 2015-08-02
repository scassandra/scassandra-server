# Scassandra - Making testing Cassandra easy

![TravisCI](https://travis-ci.org/scassandra/scassandra-server.svg?branch=master) Scassandra Server

Datastax Java 2.2, 2.1, 2.0 and 1.0 support. 

Stubbed Cassandra runs as a separate process that your application will believe is a real Cassandra node. 
It does this by implementing the server side of the binary protocol. 
It allows you to create scenarios like read time outs, write time outs and unavailable exceptions so you can test your application.

[See web site for documentation](http://www.scassandra.org/).

For feature requests and bug reports send an email to: Christopher.batey@gmail.com or on twitter: @chbatey

## Contributing

We'd love to have more contributors so get stuck in!

The whole project is built and tested with gradle. Imports into IntelliJ (Scala plugin required) without any extra config.
 
To run all tests:

```
./gradlew clean check
```

It is made up of:

* server - accepts requests over the native protocol and priming over JSON/HTTP. Written in Scala
* java-client - wrapper for starting the sever in the same JVM as unit/integration tests and a Java API for the priming endpoinds
* java-it-tests - integration tests against multiple versions of the DataStax Java driver

For any features you add please make them available in the Java client. All communication between the Java Client and the server is over HTTP.

Beware that there are deprecation warnings in the integration tests. Don't fix these as I still want to test
deprecated APIs.

### PR guidelines

* Squash your commits so that each commit is fully functional and passes all tests, including any new API exposed via the Java client
* Any format changes please put in a separate commit
* All features should be unit tested in the server and have an integration test
* Raise a separate PR for the [docs](https://github.com/scassandra/scassandra-docs) 
