import sbt._
import Keys._

object Dependencies {

  object Version {
    val scalaz        = "7.1.2"
    val scalazStream  = "0.7a"
    val argonaut      = "6.1"
    val awsScala      = "2.0.1"
    val kadai         = "3.3.0"
    val specs2        = "3.6"
    val scalacheck    = "1.12.2"
    val junit         = "4.11"
    val log4j         = "2.0.1"
    val nscalaTime    = "2.0.0"
    val healthCheck   = "1.1.0"
    val dispatch      = "0.11.2"
  }

  lazy val scalaz =
    Seq(
      "org.scalaz"               %% "scalaz-core"       % Version.scalaz
    , "org.scalaz"               %% "scalaz-effect"     % Version.scalaz
    , "org.scalaz"               %% "scalaz-concurrent" % Version.scalaz
    , "org.scalaz.stream"        %% "scalaz-stream"     % Version.scalazStream
    )

  lazy val log4j =
    Seq(
      "org.apache.logging.log4j" % "log4j-api"          % Version.log4j
    , "org.apache.logging.log4j" % "log4j-core"         % Version.log4j
    , "org.apache.logging.log4j" % "log4j-core"         % Version.log4j % "test"
    )

  lazy val kadai =
    Seq(
      "io.atlassian"             %% "kadai-core"      % Version.kadai
    ) ++ log4j


  lazy val test =
    Seq(
      "org.specs2"          %% "specs2-core"        % Version.specs2      % "test"
    , "org.specs2"          %% "specs2-junit"       % Version.specs2      % "test"
    , "org.specs2"          %% "specs2-scalacheck"  % Version.specs2      % "test"
    , "org.scalacheck"      %% "scalacheck"         % Version.scalacheck  % "test"
    , "junit"               %  "junit"              % Version.junit       % "test"
    )

  lazy val common = scalaz ++ kadai ++ test

  lazy val dispatch =
    Seq(
      "net.databinder.dispatch" %% "dispatch-core" % Version.dispatch
    )

  lazy val healthcheck = 
    Seq(
      "io.atlassian.health" %% "healthcheck-core"  % Version.healthCheck
    )

  lazy val dynamodb =
    Seq(
      "io.atlassian.aws-scala" %% "aws-scala-core"     % Version.awsScala
    , "io.atlassian.aws-scala" %% "aws-scala-dynamodb" % Version.awsScala
    , "io.atlassian.aws-scala" %% "aws-scala-core"     % Version.awsScala  % "test" classifier "tests" exclude("org.scalatest", "scalatest_2.10")
    , "io.atlassian.aws-scala" %% "aws-scala-dynamodb" % Version.awsScala  % "test" classifier "tests" exclude("org.scalatest", "scalatest_2.10")
    )

  lazy val argonaut =
    Seq(
      "io.argonaut" %% "argonaut" % Version.argonaut
    )

  lazy val nscalatime =
    Seq(
      "com.github.nscala-time" %% "nscala-time" % Version.nscalaTime
    )
}
