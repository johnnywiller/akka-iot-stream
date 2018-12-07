name := "akka-quickstart-scala"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.18"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  // https://mvnrepository.com/artifact/com.typesafe.play/play-json
  "com.typesafe.play" %% "play-json" % "2.6.10",
  // https://mvnrepository.com/artifact/com.typesafe.akka/akka-stream-kafka
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0-M1",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
)
