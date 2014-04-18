name := "Cassandra Server Stub"

version := "0.1"

scalaVersion := "2.10.3"

val sprayVersion = "1.2.0"

val akkaVersion = "2.2.+"

val testDependencies = Seq(
  "io.spray" % "spray-testkit" % sprayVersion % "test",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2+" % "test" exclude("com.google.guava", "guava"),
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.0" % "test",
  "org.scalatest" %% "scalatest" % "2.0" % "test",
  "org.mockito" % "mockito-core" % "1.9.5" % "test"
)

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "com.typesafe.akka" % "akka-actor_2.10" % akkaVersion,
  "com.typesafe.akka" % "akka-remote_2.10" % akkaVersion,
  "com.typesafe.akka" % "akka-testkit_2.10" % akkaVersion,
  "io.spray" %% "spray-json" % "1.2.5",
  "io.spray" % "spray-can" % sprayVersion,
  "io.spray" % "spray-routing" % sprayVersion,
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1"
)

libraryDependencies += "com.google.guava" % "guava" % "16.0.1" % "test" force()

libraryDependencies ++= testDependencies

scalacOptions in Test ++= Seq("-Yrangepos")

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies

resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "spray repo" at "http://repo.spray.io")

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings
