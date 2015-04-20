import sbt._
import Keys._

object Dependencies {

  lazy val SCALAZ     = "7.1.1"
  lazy val ARGONAUT   = "6.1-M6"
  lazy val KADAI      = "3.0.0"
  lazy val LOG4J      = "2.0.1"
  lazy val AWS_SCALA  = "2.0.0-M4"
  lazy val SCHMETRICS = "1.0.1"

  lazy val scalaz =
    Seq(
      "org.scalaz"               %% "scalaz-core"       % SCALAZ
    , "org.scalaz"               %% "scalaz-effect"     % SCALAZ
    , "org.scalaz"               %% "scalaz-concurrent" % SCALAZ
    , "org.scalaz.stream"        %% "scalaz-stream"     % "0.6a"
    )

  lazy val log4j =
    Seq(
      "org.apache.logging.log4j" % "log4j-api"          % LOG4J
    , "org.apache.logging.log4j" % "log4j-core"         % LOG4J
    , "org.apache.logging.log4j" % "log4j-core"         % LOG4J % "test"
    )

  lazy val kadai =
    Seq(
      "io.atlassian"             %% "kadai-core"      % KADAI
    ) ++ log4j


  lazy val test =
    Seq(
      "org.specs2"               %% "specs2"       % "2.4.9"       % "test"
    , "org.scalacheck"           %% "scalacheck"   % "1.11.6"      % "test"
    , "junit"                     %  "junit"       % "4.11"        % "test"
    )

  lazy val common = scalaz ++ kadai ++ test

  lazy val dispatch =
    Seq(
      "net.databinder.dispatch" %% "dispatch-core" % "0.11.2"
    )

  lazy val healthcheck = 
    Seq(
      "io.atlassian.health" %% "healthcheck-core"  % "1.0.0"
    )

  lazy val dynamodb =
    Seq(
      "io.atlassian.aws-scala" %% "aws-scala-core"     % AWS_SCALA
    , "io.atlassian.aws-scala" %% "aws-scala-dynamodb" % AWS_SCALA
    , "io.atlassian.aws-scala" %% "aws-scala-core"     % AWS_SCALA  % "test" classifier "tests" exclude("org.scalatest", "scalatest_2.10")
    , "io.atlassian.aws-scala" %% "aws-scala-dynamodb" % AWS_SCALA  % "test" classifier "tests" exclude("org.scalatest", "scalatest_2.10")
    )

  lazy val argonaut =
    Seq(
      "io.argonaut" %% "argonaut" % ARGONAUT
    )

  lazy val nscalatime =
    Seq(
      "com.github.nscala-time" %% "nscala-time" % "1.8.0"
    )
}
