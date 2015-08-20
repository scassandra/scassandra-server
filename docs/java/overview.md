## Overview

So far we have tested Scassandra with the following drivers:

* Datastax Java Driver 1
* Datastax Java Driver 2
* Datastax Java Driver 2.1

The next Driver we intent to test with is:

* Astyanax


***Why does the driver matter?***  Because Scassandra is pretending to be Cassandra and the driver what we need to fool!
Each driver does different queries on start up and expects slightly different responses. Fortunately the Datstax
Java Drivers don't require anything to be in the system keyspace, if there is nothing there it just carries on. However
the Python Datastax Driver blows up if no rows come back from its query on system.local.

### Getting started

The Scassandra Java Client is in Maven central. You can add it as a dependency:

### Gradle:

~~~ groovy
dependencies {
  testCompile 'org.scassandra:java-client:0.6.0'
}
~~~


### Maven:

~~~ xml
<dependency>
  <groupId>org.scassandra</groupId>
  <artifactId>java-client</artifactId>
  <version>0.6.0</version>
  <scope>test</scope>
</dependency>
~~~

If you have dependency clashes with Guava, Apache Http Client etc try the standalone version:

~~~ xml
<dependency>
  <groupId>org.scassandra</groupId>
  <artifactId>java-client</artifactId>
  <version>0.6.0</version>
  <classifier>standalone</classifier>
  <scope>test</scope>
</dependency>
~~~

There are four important classes you'll deal with from Java:

* **ScassandraFactory** - used to create instances of Scassandra
* **Scassandra** - interface for starting/stopping Scassandra and getting hold of a PrimingClient and an ActivityClient
* **PrimingClient** - sends priming requests to Scassandra RESTful admin interface
* **ActivityClient** - retrieves all the recorded queries and prepared statements from the Scassandra RESTful admin interface

The PrimingClient and ActivityClient have been created to ease integration for Java developers. Otherwise you would need to construct JSON and send it over HTTP to Scassandra.

You can start a Scassandra instance per unit test and clear all primes and recorded activity between tests.

~~~java
private static PrimingClient primingClient;
private static ActivityClient activityClient;
private static Scassandra scassandra;


@BeforeClass
public static void startScassandraServer() throws Exception {
   scassandra = ScassandraFactory.createServer();
   scassandra.start();
   primingClient = scassandra.primingClient();
   activityClient = scassandra.activityClient();
}
~~~

You can also add a AfterClass to close Scassandra down:

~~~java
 @AfterClass
 public static void shutdown() {
     scassandra.stop();
 }
~~~


### Example project

To see how to use Scassandra in your Java unit and integration tests see the following example project:

[Scassandra Java Example](https://github.com/chbatey/scassandra-example-java)

And particularly the ExampleDaoTest:

[Example JUnit test](https://github.com/chbatey/scassandra-example-java/blob/master/src/test/java/com/batey/examples/scassandra/PersonDaoTest.java)

