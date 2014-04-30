name := "scassandra-server"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

val sprayVersion = "1.2.0"

val akkaVersion = "2.2.+"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "com.typesafe.akka" % "akka-actor_2.10" % akkaVersion,
  "com.typesafe.akka" % "akka-remote_2.10" % akkaVersion,
  "io.spray" %% "spray-json" % "1.2.5",
  "io.spray" % "spray-can" % sprayVersion,
  "io.spray" % "spray-routing" % sprayVersion,
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1"
)

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "spray repo" at "http://repo.spray.io"
)

net.virtualvoid.sbt.graph.Plugin.graphSettings

publishTo := Some(Resolver.file("file",  new File( "../scassandra-repo/snapshots" )) )

pomExtra :=
<licenses>
  <license>
    <name>Apache 2</name>
    <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    <distribution>repo</distribution>
  </license>
</licenses>

/*
 *
 * test specific config
 *
 */
libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-testkit_2.10" % akkaVersion % "test",
  "io.spray" % "spray-testkit" % sprayVersion % "test",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2+" % "test" exclude("com.google.guava", "guava"),
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.0" % "test",
  "org.scalatest" %% "scalatest" % "2.0" % "test",
  "org.mockito" % "mockito-core" % "1.9.5" % "test",
  "com.google.guava" % "guava" % "16.0.1" % "test" force()
)

scalacOptions in Test ++= Seq("-Yrangepos")

parallelExecution in Test := false
/*
 *
 * end of test specific config
 *
 */