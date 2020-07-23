enablePlugins(JavaAppPackaging)

name := "dbmigrate"

version := "0.1"

scalaVersion := "2.13.2"

maintainer := "i@stingr.net"

organization := "org.stingray.contester"

scalacOptions ++= Seq(
  "-Xfatal-warnings",  // New lines for each options
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-opt:l:method",
  "-opt:l:inline",
  "-opt-inline-from:<sources>"
)

updateOptions := updateOptions.value.withCachedResolution(true)

resolvers ++= Seq(
  Resolver.jcenterRepo,
  Resolver.sonatypeRepo("snapshots"),
  Resolver.typesafeRepo("releases")
)

val slickPG = "0.19.0"

libraryDependencies ++= Seq(
  "com.github.tototoshi" %% "slick-joda-mapper" % "2.4.2",
  "org.scala-lang.modules" %% "scala-async" % "0.10.0",
  "org.clapper" %% "grizzled-slf4j" % "1.3.4",
  "com.github.nscala-time" %% "nscala-time" % "2.24.0",
  "org.mariadb.jdbc" % "mariadb-java-client" % "2.6.0",
  "org.clapper" %% "avsl" % "1.1.0",
  "com.typesafe.slick" %% "slick" % "3.3.2",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2",
  "com.typesafe" % "config" % "1.4.0",
  "info.faljse" % "SDNotify" % "1.3",
  "org.postgresql" % "postgresql" % "42.2.12",
  "com.github.tminglei" %% "slick-pg" % slickPG,
  "com.github.tminglei" %% "slick-pg_joda-time" % slickPG,
  "com.github.tminglei" %% "slick-pg_play-json" % slickPG
).map(_.exclude("org.slf4j", "slf4j-jdk14")).map(_.exclude("org.slf4j", "slf4j-log4j12"))
