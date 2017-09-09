import Dependencies._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

lazy val root = (project in file("."))
  .aggregate(server, codec, cqlAntlr, javaClient, itTestCommon, itTestCommonV3, itTestDriver3, itTestDriver20, itTestDriver21)
  .settings(
    inThisBuild(List(
      organization := "org.scassandra",
      scalaVersion := "2.12.3",
      version := "2.0.0-SNAPSHOT",
      crossScalaVersions := Seq("2.12.3", "2.11.11")
    )),
    name := "scassandra",
    publishArtifact := false

  )
  .disablePlugins(sbtassembly.AssemblyPlugin)

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

val assemblySettings = Seq(assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
},
  test in assembly := {},
  assemblyJarName := s"${name.value}-standalone-${version.value}.jar",
  artifact in(Compile, assembly) := {
    val art = (artifact in(Compile, assembly)).value
    art.copy(`classifier` = Some("standalone"))
  }
)

val formatPreferences =
  ScalariformKeys.preferences := ScalariformKeys.preferences.value
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(DanglingCloseParenthesis, Preserve)
    .setPreference(DoubleIndentConstructorArguments, false)

lazy val server = (project in file("server"))
  .settings(
    name := "scassandra-server",
    scalaVersion := "2.12.3",
    libraryDependencies ++= serverDeps,
    libraryDependencies ++= serverTestDeps,
    (testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-h", "server/target/report"),
    // Crappy, need to fix the e2e tests to use different ports
    parallelExecution in Test := false
  )
  .settings(addArtifact(artifact in(Compile, assembly), assembly).settings: _*)
  .settings(formatPreferences)
  .settings(assemblySettings)
  .dependsOn(codec, cqlAntlr)
  .enablePlugins(SbtScalariform)

lazy val javaClient = (project in file("java-client"))
  .settings(
    name := "java-client",
    libraryDependencies ++= clientDeps,
    libraryDependencies ++= clientTestDeps,
    parallelExecution in Test := false
  )
  .settings(addArtifact(artifact in(Compile, assembly), assembly).settings: _*)
  .settings(assemblySettings)
  .dependsOn(cqlAntlr, server)

lazy val codec = (project in file("codec"))
  .settings(
    name := "scassandra-codec",
    libraryDependencies ++= codecDeps,
    libraryDependencies ++= codecTestDeps
  )
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val cqlAntlr = (project in file("cql-antlr"))
  .settings(
    name := "cql-antlr",
    libraryDependencies ++= cqlDeps,
    libraryDependencies ++= cqlTestDeps,
    antlr4Settings,
    antlr4PackageName in Antlr4 := Some("org.scassandra.antlr4"),
    antlr4Dependency in Antlr4 := antlr
  )
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val itTestCommon = (project in file("java-it-tests/common"))
  .settings(
    libraryDependencies ++= itTestsCommonDeps,
    publishArtifact := false
  )
  .dependsOn(javaClient)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val itTestCommonV3 = (project in file("java-it-tests/common-v3"))
  .settings(
    libraryDependencies ++= itTestsCommonV3Deps,
    publishArtifact := false
  )
  .dependsOn(javaClient, itTestCommon)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val itTestDriver20 = (project in file("java-it-tests/driver20"))
  .settings(
    libraryDependencies ++= itTestsDriver20Deps,
    parallelExecution in Test := false,
    publishArtifact := false
  )
  .dependsOn(javaClient, itTestCommon)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val itTestDriver21 = (project in file("java-it-tests/driver21"))
  .settings(
    libraryDependencies ++= itTestsDriver21Deps,
    parallelExecution in Test := false,
    publishArtifact := false
  )
  .dependsOn(javaClient, itTestCommon, itTestCommonV3)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val itTestDriver3 = (project in file("java-it-tests/driver30"))
  .settings(
    libraryDependencies ++= itTestsDriver30Deps,
    parallelExecution in Test := false,
    publishArtifact := false
  )
  .dependsOn(javaClient, itTestCommon, itTestCommonV3)
  .disablePlugins(sbtassembly.AssemblyPlugin)
