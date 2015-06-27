# Scassandra - Making testing Cassandra easy

![TravisCI](https://travis-ci.org/scassandra/scassandra-server.svg?branch=master) Scassandra Server

Datastax Java 2.1 and 2.0 support. Version 0.7.0 onward do not support version 1.0 of the driver.

Stubbed Cassandra runs as a separate process that your application will believe is a real Cassandra node. It does this by implementing the server side of the binary protocol. It allows you to create scenarios like read time outs, write time outs and unavailable exceptions so you can test your application.

[See web site for documentation](http://www.scassandra.org/).

For feature requests and bug reports send an email to: Christopher.batey@gmail.com or on twitter: @chbatey

## Contribution

The server component is written in Scala and built with SBT.

For any features you add please make them available in the Java client. All communication between the Java Client and the server is over HTTP.

Integration tests for different versions of the DataStax Java Driver are in the java-it-tests folder.## Contribution

Beware that there are deprecation warnings in the integration tests. Don't fix these as I still want to test
deprecated APIs.
