import sbt._, Keys._
import sbtrelease.ReleasePlugin._
import com.typesafe.sbt.SbtScalariform._
import wartremover._

object Settings {
  val scalacFlags =  Seq(
    "-deprecation"
  , "-encoding", "UTF-8" // yes, this is 2 args
  , "-target:jvm-1.7"
  , "-feature"
  , "-language:existentials"
  , "-language:experimental.macros"
  , "-language:higherKinds"
  , "-language:implicitConversions"
  , "-language:reflectiveCalls"
  , "-unchecked"
  , "-Xfatal-warnings"
//  , "-Xlint" // commented out due to https://issues.scala-lang.org/browse/SI-8476
  , "-Yno-adapted-args"
  //, "-Ywarn-all"  // Doesn't work with scala 2.11
  , "-Ywarn-dead-code" // N.B. doesn't work well with the ??? hole
  , "-Ywarn-numeric-widen"
  , "-Ywarn-value-discard"
  , "-Xmax-classfile-name", "134"
  )

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    releaseSettings ++ // sbt-release
    wartRemoval ++
    scalariformSettings ++
    scalariformPrefs ++
    Seq[Def.Setting[_]] (
      organization := "io.atlassian"
    , pomIncludeRepository := { (repo: MavenRepository) => false } // no repositories in the pom
    , scalaVersion := "2.11.8"
    , crossScalaVersions  := Seq("2.11.8", "2.10.6")
    , ReleaseKeys.crossBuild := true
    , autoScalaLibrary := false
    , ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
    , scalacOptions ++= scalacFlags
    , javacOptions ++= Seq("-encoding", "UTF-8")
    , resolvers ++= Seq(
        Resolver.defaultLocal
      , Resolver.mavenLocal
      , "atlassian-public"   at "https://packages.atlassian.com/maven/repository/public/"
      , Resolver.sonatypeRepo("public")
      , Resolver.sonatypeRepo("releases")
      , Resolver.sonatypeRepo("snapshots")
      , Resolver.bintrayRepo("non", "maven")
      , Resolver.bintrayRepo("scalaz", "releases")
      )
    , mappings in (Compile, packageBin) ++= Seq(
        file("LICENSE") -> "META-INF/LICENSE"
      )
    , credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
    , addCompilerPlugin("org.scalamacros"   % "paradise"         % "2.1.0" cross CrossVersion.full)
    , addCompilerPlugin("org.spire-math"    % "kind-projector"   % "0.9.3" cross CrossVersion.binary)
    , addCompilerPlugin("com.milessabin"    % "si2712fix-plugin" % "1.2.0" cross CrossVersion.full)
    )

  lazy val scalariformPrefs = {
    import scalariform.formatter.preferences._

    Seq[Def.Setting[_]](
      ScalariformKeys.preferences := ScalariformKeys.preferences.value
        .setPreference(AlignArguments,                    false) // scalariform 0.1.4
        .setPreference(AlignParameters,                   false)
        .setPreference(AlignSingleLineCaseStatements,     true)
        .setPreference(CompactControlReadability,         true)
        .setPreference(CompactStringConcatenation,        true)
        .setPreference(DoubleIndentClassDeclaration,      true)
        .setPreference(PreserveSpaceBeforeArguments,      true)
        .setPreference(RewriteArrowSymbols,               false)
        .setPreference(SpaceInsideParentheses,            false)
        .setPreference(SpacesAroundMultiImports,          true) // scalariform 0.1.4
        )
  }

  lazy val wartRemoval =
    wartremoverSettings ++ Seq(
      wartremoverErrors in (Compile, compile) ++=
        Warts.allBut(
          Wart.Any
          , Wart.DefaultArguments
          , Wart.NoNeedForMonad
          , Wart.NonUnitStatements
          , Wart.Nothing
          , Wart.Throw
          , Wart.Product
          , Wart.Serializable
          , Wart.FinalCaseClass
        )
      , wartremoverExcluded ++= Seq()
    )
}
