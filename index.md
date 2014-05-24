---
layout: default
---

###Overview

Scassandra server is written in Scala but we also have a Java wrapper for use with Java unit and integration tests.
If you're looking to test your Cassandra application then go to the [Java Client](www.scassandra.org/java-lcient).

Version 0.1 of Scassandra is primarily aimed at Java developers. The next version will have more focus on the standalone version of Scassandra.

However if you want to use Scassandra from a language/system other than Java, and you don't want to wait for version 0.2 then you are in the right place.

Currently there is no binary release of Scassandra. To run as a standalone server you need to checkout the project and run:

```
sbt assembly
```

Once you've assembled the project the server is an executable jar file and can be started with the following command:

```
java -jar cassandra_server_stub.jar
```

The default ports are:

* Cassandra Binary Port: 8042
* Admin/Priming Port: 8043

To override the ports / log level use the following java properties:
```
-Dscassandra.binary.port=1234
-Dscassandra.binary.port=4566
-Dscassandra.log.level=DEBUG
```

E.g
```
java -jar  -Dscassandra.binary.port=1234 -Dscassandra.binary.port=4566 -Dscassandra.log.level=DEBUG cassandra_server_stub.jar
```

Once you have the server up and running see all the pages in the menu for how to prime/verify activity.




