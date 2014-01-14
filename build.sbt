name := "Cassandra Server Stub"

version := "0.1"

scalaVersion := "2.10.2"


libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "1.9.2" % "test",
    "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "com.typesafe.akka" % "akka-actor_2.10" % "2.2.+",
    "com.typesafe.akka" % "akka-remote_2.10" % "2.2.+",
    "com.typesafe.akka" % "akka-testkit_2.10" % "2.2.+",
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.0+"
)

scalacOptions in Test ++= Seq("-Yrangepos")

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies

resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "releases"  at "http://oss.sonatype.org/content/repositories/releases")

parallelExecution in Test := false
