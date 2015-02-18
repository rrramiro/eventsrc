import sbt._
import sbt.Keys._
import Settings._

trait Modules {
  val project: String => Project = n => 
    Project(
      id = n
    , base = file(n)
    , settings = standardSettings
    ).settings(
      name := s"eventsrc-$n"
    , libraryDependencies ++= Dependencies.common
    )

  lazy val core = project("core")

  lazy val dynamo = project("dynamodb").dependsOn(core)

  // aggregate
  lazy val all = 
    Project(id = "eventsrc"
    , base = file(".")
    , settings = standardSettings
    ) dependsOn ( // needs both dependsOn and aggregate to produce dependencies in the pom
      core, dynamo
    ) aggregate (
      core, dynamo
    )
}
