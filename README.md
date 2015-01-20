# Scassandra - Making testing Cassandra easy

![TravisCI](https://travis-ci.org/scassandra/scassandra-server.svg?branch=master) Scassandra Server

![TravisCI](https://travis-ci.org/scassandra/scassandra-java-client.svg?branch=master) Java Client


![TravisCI](https://travis-ci.org/scassandra/scassandra-it-java-driver-2.svg?branch=master) Datastax Java 2.0.* Integration tests AND Datastax Java 1.* Intregration tests


Datastax Java 2.1 support is in master but not released yet.

Stubbed Cassandra runs as a separate process that your application will believe is a real Cassandra node. It does this by implementing the server side of the binary protocol. It allows you to create scenarios like read time outs, write time outs and unavailable exceptions so you can test your application.

[See web site for documentation](http://www.scassandra.org/).

For feature requests and bug reports send an email to: Christopher.batey@gmail.com or on twitter: @chbatey


