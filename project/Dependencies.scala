import sbt._

object Dependencies {

  val antlrVersion = "4.5.3"
  val akkaVersion = "2.5.3"
  val akkaHttpVersion = "10.0.9"
  val commonsCodecVersion = "1.10"
  val httpClientVersion = "4.3.3"
  val javaDriverVersion = "3.1.1"
  val junitVersion = "4.11"
  val guavaVersion = "17.0"
  val gsonVersion = "2.5"
  val logbackVersion = "1.1.1"
  val mockitoVersion = "1.9.5"
  val scodecVersion = "1.10.3"
  val slf4jVersion = "1.7.10"
  val catsVersion = "1.0.0-MF"

  val cats = "org.typelevel" %% "cats-core" % catsVersion

  val logback = "ch.qos.logback" % "logback-classic" % logbackVersion

  val akkaHttpCors = "ch.megard" %% "akka-http-cors" % "0.2.1"
  val akkaHttp = "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
  val akkaHttpSpray = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
  val akkaActor = "com.typesafe.akka" %% "akka-actor" % akkaVersion
  val akkaTyped = "com.typesafe.akka" %% "akka-typed" % akkaVersion
  val akkaRemote = "com.typesafe.akka" %% "akka-remote" % akkaVersion
  val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
  val typesafeLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
  val scodec = "org.scodec" %% "scodec-core" % scodecVersion

  val guava = "com.google.guava" % "guava" % guavaVersion
  val antlrRuntime = "org.antlr" % "antlr4-runtime" % antlrVersion
  val antlr = "org.antlr" % "antlr4" % antlrVersion
  val slf4jApi = "org.slf4j" % "slf4j-api" % slf4jVersion
  val commonsCodec = "commons-codec" % "commons-codec" % commonsCodecVersion
  val httpClient = "org.apache.httpcomponents" % "httpclient" % httpClientVersion
  val gson = "com.google.code.gson" % "gson" % gsonVersion

  val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
  val akkaHttpTestkit = "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion
  val akkaTypedTestKit = "com.typesafe.akka" %% "akka-typed-testkit" % akkaVersion

  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
  val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.13.5"
  val pegdown = "org.pegdown" % "pegdown" % "1.6.0"

  val junit = "junit" % "junit" % junitVersion
  val mockito = "org.mockito" % "mockito-core" % mockitoVersion
  val cassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0" //exclude("com.google.guava", "guava"),
  // todo exclude slf4j-log4j12
  val cassandraDriver21 = "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.10.2" exclude ("log4j", "log4j")
  val cassandraDriver20 = "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.12.3" exclude ("log4j", "log4j")
  val wiremock = "com.github.tomakehurst" % "wiremock" % "1.46"
  val equalsVerifier = "nl.jqno.equalsverifier" % "equalsverifier" % "1.7.3"
  val jarjar = "com.googlecode.jarjar" % "jarjar" % "1.3"
  val junitInterface = "com.novocode" % "junit-interface" % "0.11"

  val serverDeps = Seq(cats, antlrRuntime, logback, akkaHttpCors, akkaHttp, akkaHttpSpray, akkaActor, akkaRemote, akkaSlf4j, akkaTyped, typesafeLogging, guava)
  val serverTestDeps = Seq(akkaTestkit, akkaHttpTestkit, akkaTypedTestKit, scalaTest, mockito, cassandraDriver, pegdown).map(_ % "test")

  val codecDeps = Seq(scodec, guava)
  val codecTestDeps = Seq(scalaTest, scalaCheck).map(_ % "test")

  val cqlDeps = Seq(antlr, antlrRuntime, slf4jApi, commonsCodec, guava)
  val cqlTestDeps = Seq(junit, mockito, junitInterface).map(_ % "test")

  val clientDeps = Seq(httpClient, gson, slf4jApi, junit)
  val clientTestDeps = Seq(logback, wiremock, equalsVerifier, jarjar, cassandraDriver, junitInterface, pegdown).map(_ % "test")

  val itTestsCommonDeps = Seq(guava, junitInterface)
  val itTestsCommonV3Deps = Seq(guava, cassandraDriver21, junitInterface)

  val itTestsDriver20Deps = Seq(cassandraDriver20, junitInterface)
  val itTestsDriver21Deps = Seq(cassandraDriver21, junitInterface)
  val itTestsDriver30Deps = Seq(cassandraDriver, junitInterface)
}
