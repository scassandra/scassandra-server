name := "Cassandra Server Stub"

version := "0.1"

scalaVersion := "2.10.2"


libraryDependencies ++= Seq(
    "org.specs2" %% "specs2" % "2.2.3" % "test"
)

scalacOptions in Test ++= Seq("-Yrangepos")

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies

resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "releases"  at "http://oss.sonatype.org/content/repositories/releases")