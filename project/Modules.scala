import sbt._
import sbt.Keys._
import Settings._

trait Modules {
  def project(n: String, deps: Seq[ModuleID] = Vector()): Project = 
    Project(
      id = n
    , base = file(n)
    , settings = standardSettings
    ).settings(
      name := s"eventsrc-$n"
    , libraryDependencies ++= (Dependencies.common ++ deps)
    )

  lazy val core = project("core")

  lazy val dynamo = project("dynamodb", Dependencies.dynamodb).dependsOn(core)

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
