---
layout: default
---

###Overview

Scassandra server is written in Scala but we also have a Java wrapper for use with Java unit and integration tests.
If you're looking to test your Cassandra application from Java then go to the [Java Client](http://www.scassandra.org/java-client).

The current version of Scassandra is primarily aimed at Java developers. The next version will have more focus on the standalone version of Scassandra.

However if you want to use Scassandra from a language/system other than Java, and you don't want to wait for version 1.0 then you are in the right place.

You'll need to checkout ```https://github.com/scassandra/scassandra-server``` and run
```
sbt assembly
```

This will buid you a fat jar that you can start with:

```
java -jar <jar>
```

The default ports are:

* Cassandra Binary Port: 8042
* Admin/Priming Port: 8043

To override the ports / log level use the following java properties:

```
-Dscassandra.binary.port=1234
-Dscassandra.binary.admin=4566
-Dscassandra.log.level=DEBUG
```

E.g

```
java -jar -Dscassandra.binary.port=1234 -Dscassandra.admin.port=4566 -Dscassandra.log.level=INFO scassandra-server-all_2.10-0.2.0.jar
```

Once you have the server up and running see all the pages in the menu for how to prime/verify activity.




