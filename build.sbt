import Settings.standardSettings

lazy val depTest = "test->test;compile->compile"

def project(n: String, deps: Seq[ModuleID] = Vector()): Project =
  Project(id = n , base = file(n)).enablePlugins(ReleasePlugin).settings(standardSettings).settings(
    name := s"eventsrc-$n"
    , libraryDependencies ++= (Dependencies.common ++ deps)
  )

lazy val core = project("core", Dependencies.nscalatime ++ Dependencies.argonaut)

lazy val coreTest = core % depTest

lazy val dynamo = project("dynamodb", Dependencies.dynamodb).dependsOn(coreTest).settings(parallelExecution in Test := false)

// aggregate
lazy val all =
  Project(id = "eventsrc"
    , base = file(".")
  ).settings(standardSettings).dependsOn ( // needs both dependsOn and aggregate to produce dependencies in the pom
    core, dynamo
  ) aggregate (
    core, dynamo
  )

Global / onChangedBuildSource := ReloadOnSourceChanges