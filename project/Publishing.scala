// Copyright 2012 Atlassian PTY LTD
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import sbt._
import Keys._

object Publishing extends Plugin {
  val nexus = "https://maven.atlassian.com/"
  lazy val release = Some("releases" at nexus + "public")
  lazy val snapshots = Some("snapshots" at nexus + "public-snapshot")
  lazy val local = Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

  override def settings = 
    Seq(
      publishTo <<= publishToPublic
    , publishMavenStyle := true
    , publishArtifact in Test := false
    , pomExtra :=
        <scm>
          <url>https://bitbucket.org/atlassianlabs/eventsrc</url>
          <connection>scm:ssh://git@bitbucket.org:atlassianlabs/eventsrc.git</connection>
          <developerConnection>scm:git:ssh://git@bitbucket.org:atlassianlabs/eventsrc.git</developerConnection>
        </scm>
          <issueManagement>
            <system>Bitbucket</system>
            <url>https://bitbucket.org/atlassianlabs/eventsrc/issues</url>
          </issueManagement>
    , pomIncludeRepository := { (repo: MavenRepository) => false } // no repositories in the pom
    )

  lazy val publishToPublic =
    version { publishForVersion(local)(release) }

  private def publishForVersion(snapshot: Option[Resolver])(release: Option[Resolver])(v: String): Option[Resolver] =
    if (v.trim endsWith "SNAPSHOT") snapshot else release
}
