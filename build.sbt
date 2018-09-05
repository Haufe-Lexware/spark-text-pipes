name := "SparkTextPipes"
organization := "com.haufe.umantis"
version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

val appsPath = sys.env.getOrElse("APPS_PATH", "../apps")

lazy val akkaKryoSerializerUpdated = RootProject(
  uri("https://github.com/Haufe-Lexware/akka-kryo-serialization.git"))

lazy val root = (project in file(".")).dependsOn(akkaKryoSerializerUpdated)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.4",
	"com.google.guava" % "guava" % "15.0", // for tests
  "io.netty" % "netty-common" % "4.1.29.Final"
)


lazy val sparkDependencies = {
  val sparkVer = "2.3.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-streaming" % sparkVer,
    "org.apache.spark" %% "spark-mllib" % sparkVer,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVer
  )
}

// spark
libraryDependencies ++= sparkDependencies.map(_ % "provided")
Test / test / libraryDependencies ++= sparkDependencies

// DEBUG LOGGING
// libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.2.3"

// Elasticsearch integration (NOTE: currently not used!)
//libraryDependencies ++= Seq(
//  "org.elasticsearch" %% "elasticsearch-spark-20" % "6.0.1"
//)

// ################################ NLP
// spark-nlp (NOTE: currently not used!)
//libraryDependencies += "JohnSnowLabs" % "spark-nlp" % "1.2.3"

// https://mvnrepository.com/artifact/edu.stanford.nlp/stanford-corenlp
libraryDependencies ++= {
  val CoreNLPver = "3.9.1"

  val jarURL = s"https://github.com/Haufe-Lexware/stanford-corenlp-shaded/raw/master/CoreNLP/target/stanford-corenlp-shaded-$CoreNLPver.jar"
  Seq(
    "edu.stanford.nlp" % "stanford-corenlp-shaded" % CoreNLPver from jarURL,
    "edu.stanford.nlp" % "stanford-corenlp" % CoreNLPver % "provided" classifier "models" exclude("com.google.protobuf", "protobuf-java"),
    "edu.stanford.nlp" % "stanford-corenlp" % CoreNLPver % "provided" classifier "models-english-kbp" exclude("com.google.protobuf", "protobuf-java"),
    "edu.stanford.nlp" % "stanford-corenlp" % CoreNLPver % "provided" classifier "models-english" exclude("com.google.protobuf", "protobuf-java"),
    "edu.stanford.nlp" % "stanford-corenlp" % CoreNLPver % "provided" classifier "models-german" exclude("com.google.protobuf", "protobuf-java"),
    "edu.stanford.nlp" % "stanford-corenlp" % CoreNLPver % "provided" classifier "models-french" exclude("com.google.protobuf", "protobuf-java")
  )
}

// https://mvnrepository.com/artifact/de.jollyday/jollyday
libraryDependencies += "de.jollyday" % "jollyday" % "0.5.1"


// DOCUMENT ANALYSIS (html to text)
// https://mvnrepository.com/artifact/org.apache.tika/tika-parsers
libraryDependencies += "org.apache.tika" % "tika-parsers" % "1.17"

// scala scraper
libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "2.0.0"

// scala json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.7"


// SPARQL queries
// https://mvnrepository.com/artifact/org.apache.jena/jena-arq
libraryDependencies += "org.apache.jena" % "jena-arq" % "3.6.0"

// language identification
// https://mvnrepository.com/artifact/com.carrotsearch/langid-java
libraryDependencies += "com.carrotsearch" % "langid-java" % "1.0.0"

// AKKA
libraryDependencies ++= {
  val akkaVer = "2.5.11"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVer,
    "com.typesafe.akka" %% "akka-remote" % akkaVer,
    "com.typesafe.akka" %% "akka-testkit" % akkaVer % Test
  )
}

// Utils from Twitter, LruMap
libraryDependencies += "com.twitter" %% "util-collection" % "18.2.0"

// BLAS
// https://mvnrepository.com/artifact/com.github.fommil.netlib/all
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()

// Scalatests
// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
logBuffered in Test := false

// Spark tests
// check project/plugins.sbt and update
libraryDependencies += ("MrPowers" % "spark-fast-tests" % "0.4.0").withSources()

// country codes
libraryDependencies += "com.vitorsvieira" %% "scala-iso" % "0.1.2"

// mongodb spark driver
libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.2.1"

// REST api
// https://mvnrepository.com/artifact/com.github.finagle/finch-core
libraryDependencies += "com.github.finagle" %% "finch-core" % "0.22.0"
// https://mvnrepository.com/artifact/com.github.finagle/finch-circe
libraryDependencies += "com.github.finagle" %% "finch-circe" % "0.22.0"

// https://mvnrepository.com/artifact/io.circe/circe-core
libraryDependencies += "io.circe" %% "circe-core" % "0.9.3"

// https://mvnrepository.com/artifact/io.circe/circe-parser
libraryDependencies += "io.circe" %% "circe-parser" % "0.9.3"

// https://mvnrepository.com/artifact/io.circe/circe-generic-extras
libraryDependencies += "io.circe" %% "circe-generic-extras" % "0.9.3"

// HTTP client
libraryDependencies +="org.dispatchhttp" %% "dispatch-core" % "0.14.0"

// KAFKA
resolvers += "confluent" at "http://packages.confluent.io/maven/"

libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "4.0.0"

// https://mvnrepository.com/artifact/com.databricks/spark-avro
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0"

// Cassandra
libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.3.0-s_2.11"

// icu4j
// https://mvnrepository.com/artifact/com.ibm.icu/icu4j
libraryDependencies += "com.ibm.icu" % "icu4j" % "62.1"

// https://mvnrepository.com/artifact/com.linkedin.urls/url-detector
libraryDependencies += "com.linkedin.urls" % "url-detector" % "0.1.17"

// https://mvnrepository.com/artifact/io.netty/netty-common
libraryDependencies += "io.netty" % "netty-common" % "4.1.29.Final"

// merge strategies for apache tika fat jar, please check
// https://stackoverflow.com/questions/47100718/apache-tika-1-16-txtparser-failed-to-detect-character-encoding-in-sbt-build
assemblyMergeStrategy in assembly := {
  case PathList("reference.conf") => MergeStrategy.concat
  case x if x.contains("EncodingDetector") => MergeStrategy.deduplicate
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.last
}

assemblyOutputPath in assembly := file(s"$appsPath/${name.value}.jar")

Project.inConfig(Test)(baseAssemblySettings)
assemblyJarName in (Test, assembly) := s"${name.value}-test-${version.value}.jar"

fork in ThisBuild in Test:= false

