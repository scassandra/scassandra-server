## Overview

So far we have tested Scassandra with the following drivers:

* DataStax Java Driver 2
* DataStax Java Driver 2.1
* DataStax Java Driver 3.0
* DataStax Java Driver 3.1

Support may be added in the future for:

* Astyanax


***Why does the driver matter?***  Because Scassandra is pretending to be Cassandra and the driver is what we need to fool!
Each driver does different queries on start up and expects slightly different responses. Fortunately the Datstax
Java Drivers don't require anything to be in the system keyspace, if there is nothing there it just carries on. 

However other drivers may not be able to connect as they depend on responses to queries on the `system` and 
`system_schema` tables to have certain payloads and column metadata present.  In future releases, Scassandra will try
to handle these queries so drivers may connect gracefully.

### Getting started

The Scassandra Java Client is available in Maven central.
See [the GitHub Releases page](https://github.com/scassandra/scassandra-server/releases) for the latest version number.

You can add it as a dependency:

### Gradle:

~~~ groovy
dependencies {
  testCompile 'org.scassandra:java-client:1.1.0'
}
~~~


### Maven:

~~~ xml
<dependency>
  <groupId>org.scassandra</groupId>
  <artifactId>java-client</artifactId>
  <version>1.1.0</version>
  <scope>test</scope>
</dependency>
~~~

If you have dependency clashes with Guava, Apache Http Client etc try the standalone version which shades these
dependencies:

~~~ xml
<dependency>
  <groupId>org.scassandra</groupId>
  <artifactId>java-client</artifactId>
  <version>1.0.0</version>
  <classifier>standalone</classifier>
  <scope>test</scope>
</dependency>
~~~

There are four important classes you'll deal with from Java:

* **ScassandraFactory** - used to create instances of Scassandra
* **Scassandra** - interface for starting/stopping Scassandra and getting hold of a PrimingClient and an ActivityClient
* **PrimingClient** - sends priming requests to Scassandra RESTful admin interface
* **ActivityClient** - retrieves all the recorded queries and prepared statements from the Scassandra RESTful admin interface
* **CurrentClient** - retrieve information about current client connections established, capability to close connections.

The PrimingClient and ActivityClient have been created to ease integration for Java developers. Otherwise you would need to construct JSON and send it over HTTP to Scassandra.

You can start a Scassandra instance per unit test and clear all primes and recorded activity between tests.

~~~java
private static PrimingClient primingClient;
private static ActivityClient activityClient;
private static CurrentClient currentClient;
private static Scassandra scassandra;


@BeforeClass
public static void startScassandraServer() throws Exception {
   scassandra = ScassandraFactory.createServer();
   scassandra.start();
   primingClient = scassandra.primingClient();
   activityClient = scassandra.activityClient();
   currentClient = scassandra.currentClient();
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

[Example JUnit test](https://github.com/chbatey/scassandra-example-java/blob/master/src/test/java/com/batey/examples/scassandra/PersonDaoCassandraTest.java)

