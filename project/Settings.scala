import sbt._, Keys._
import sbtrelease.ReleasePlugin._

object Settings {
  // "-language:_"
  //   Seq( "-Xlog-free-terms")

  val scalacFlags =  Seq(
    "-deprecation"
  , "-encoding", "UTF-8" // yes, this is 2 args
  , "-target:jvm-1.7"
  , "-feature"
  , "-language:existentials"
  , "-language:experimental.macros"
  , "-language:higherKinds"
  , "-language:implicitConversions"
  , "-unchecked"
  , "-Xfatal-warnings"
//  , "-Xlint" // commented out due to https://issues.scala-lang.org/browse/SI-8476
  , "-Yno-adapted-args"
  //, "-Ywarn-all"
  , "-Ywarn-dead-code" // N.B. doesn't work well with the ??? hole
  , "-Ywarn-numeric-widen"
  , "-Ywarn-value-discard"     
  )

  lazy val standardSettings = 
    Defaults.coreDefaultSettings ++ 
    releaseSettings ++ // sbt-release
    net.virtualvoid.sbt.graph.Plugin.graphSettings ++ // dependency plugin settings 
    Seq[Def.Setting[_]] (
      organization := "io.atlassian"
    , pomIncludeRepository := { (repo: MavenRepository) => false } // no repositories in the pom
    , scalaVersion := "2.11.5"
    , crossScalaVersions  := Seq("2.11.5", "2.10.4")
    , ReleaseKeys.crossBuild := true
    , autoScalaLibrary := false
    , scalacOptions ++= scalacFlags 
    , javacOptions ++= Seq("-encoding", "UTF-8")
    , resolvers ++= Seq(
        Resolver.mavenLocal
      , "Tools Snapshots"    at "http://oss.sonatype.org/content/repositories/snapshots"
      , "Tools Releases"     at "http://oss.sonatype.org/content/repositories/releases"
      , "atlassian-public"   at "https://maven.atlassian.com/content/groups/atlassian-public/"
      , "atlassian-internal" at "https://maven.atlassian.com/content/groups/internal/"
      )
    , mappings in (Compile, packageBin) ++= Seq(
        file("LICENSE") -> "META-INF/LICENSE"
      , file("NOTICE")  -> "META-INF/NOTICE"
      )
    , credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
    , addCompilerPlugin("org.scalamacros"        % "paradise"       % "2.0.1" cross CrossVersion.full)
    )
}
