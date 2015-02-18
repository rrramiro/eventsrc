import sbt._
import sbt.Process
import sbtrelease.ReleasePlugin.{ReleaseKeys, releaseSettings}
import sbtrelease.ReleasePlugin.ReleaseKeys._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease._
import sbt.Keys._

object Release {

  private val mavenCommand = "mvn3"

  private lazy val releaseProcess = Seq[ReleaseStep](
    checkMavenExists,
    checkSnapshotDependencies,
    inquireVersions,
    runClean,

    setReleaseVersion,
    setServiceDescriptorVersion,
    setMavenClientPluginVersion,
    stageFilesWithModifiedVersions,
    commitReleaseVersion,

    tagRelease,

    publishArtifacts,
    publishMavenClientPlugin,

    setNextVersion,
    setServiceDescriptorVersion,
    setMavenClientPluginVersion,
    stageFilesWithModifiedVersions,
    commitNextVersion,

    pushChanges
  )

  /* --- Release Steps --- */

  private lazy val setServiceDescriptorVersion = ReleaseStep(action = st => {
    val extracted = Project.extract(st)

    // Get the project version
    val version = extracted.get(Keys.version)
    st.log.info(s"Updating service descriptor version to $version")

    // Get the parent root where the script to update the service descriptor version is
    val rootDirectory = extracted.get(Keys.baseDirectory)
    Process(Seq("bash", "-c", s"${rootDirectory.getAbsolutePath}/project/bin/updateServiceDescriptorVersion.sh $version")) !! st.log
    st
  })

  private lazy val checkMavenExists = ReleaseStep { st =>
    st.log.debug(s"Checking that Maven is installed")
    Process(Seq(mavenCommand, "-version")) !! st.log
    st
  }

  private lazy val setMavenClientPluginVersion = ReleaseStep { st =>
    val extracted = Project.extract(st)
    // Get the project version
    val version = extracted.get(Keys.version)
    st.log.info(s"Updating Maven Client Plugin version to $version")
    Process(Seq(mavenCommand, "versions:set", s"-DnewVersion=$version", "-DgenerateBackupPoms=false")) ! st.log
    st
  }

  private lazy val publishMavenClientPlugin = ReleaseStep { st =>
    st.log.info("Compiling and deploying Maven Client Plugin")
    Process(Seq(mavenCommand, "-U", "clean", "verify", "deploy")) ! st.log
    st
  }

  private def vcs(st: State): Vcs = {
    import Utilities._
    st.extract.get(versionControlSystem).getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
  }

  private lazy val stageFilesWithModifiedVersions = ReleaseStep { st =>
    vcs(st).cmd("add", "-u") !! st.log
    st
  }

  private lazy val customVcsMessages = Seq(
    tagComment    <<= (version in ThisBuild) map { v => "[sbt-release] Releasing %s" format v }
    , commitMessage <<= (version in ThisBuild) map { v => "[sbt-release] Setting version to %s" format v }
  )

  lazy val customReleaseSettings =
    releaseSettings ++
    Seq(
      ReleaseKeys.releaseProcess := Release.releaseProcess
      , nextVersion    := { ver => Version(ver).map(_.bumpBugfix.asSnapshot.string).getOrElse(versionFormatError) } // bump patch numbers
    ) ++
    customVcsMessages
}