import sbt._
import Keys._

object Dependencies {

  object Version {
    val scalaz        = "7.2.15"
    val scalazStream  = "0.8"
    val monocle       = "1.2.2"
    val argonaut      = "6.2"
    val awsScala      = "7.0.6"
    val kadai         = "5.0.0"
    val specs2        = "3.8.3"
    val scalacheck    = "1.13.1"
    val junit         = "4.12"
    val log4j         = "2.0.1"
    val nscalaTime    = "2.0.0"
  }

  lazy val scalaz =
    Seq(
      "org.scalaz"               %% "scalaz-core"       % Version.scalaz
    , "org.scalaz"               %% "scalaz-effect"     % Version.scalaz
    , "org.scalaz"               %% "scalaz-concurrent" % Version.scalaz
    , "org.scalaz.stream"        %% "scalaz-stream"     % Version.scalazStream
    )

  lazy val monocle =
    Seq(
      "com.github.julien-truffaut"  %%  "monocle-core"    % Version.monocle,
      "com.github.julien-truffaut"  %%  "monocle-generic" % Version.monocle,
      "com.github.julien-truffaut"  %%  "monocle-macro"   % Version.monocle,
      "com.github.julien-truffaut"  %%  "monocle-state"   % Version.monocle,
      "com.github.julien-truffaut"  %%  "monocle-law"     % Version.monocle % "test"
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
      "org.specs2"          %% "specs2-core"          % Version.specs2      % "test"
    , "org.specs2"          %% "specs2-junit"         % Version.specs2      % "test"
    , "org.specs2"          %% "specs2-scalacheck"    % Version.specs2      % "test"
    , "org.specs2"          %% "specs2-matcher-extra" % Version.specs2      % "test"
    , "org.scalacheck"      %% "scalacheck"           % Version.scalacheck  % "test"
    , "junit"               %  "junit"                % Version.junit       % "test"
    )

  lazy val common = scalaz ++ monocle ++ kadai ++ test

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
