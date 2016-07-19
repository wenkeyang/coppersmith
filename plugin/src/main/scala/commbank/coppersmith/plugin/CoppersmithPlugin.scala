//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith.plugin

import sbt._
import Keys._

object CoppersmithPlugin extends AutoPlugin {

  object autoImport {
    lazy val metadata = TaskKey[File]("export")
    lazy val Metadata = config("metadata") extend Runtime
  }

  import autoImport._

  lazy val baseMetadataSettings: Seq[sbt.Def.Setting[_]] = Seq(
    scalaVersion := "2.11.8"
  ) ++ Seq(
    artifacts += Artifact(name.value, "metadata", "json"),
    packagedArtifacts := packagedArtifacts.value.updated(
      Artifact(name.value, "metadata", "json"),
      (metadata in Metadata).value
    )
  ) ++
  inConfig(Metadata)(
    Seq(
      metadata <<= (
        libraryDependencies in Metadata,
        fullClasspath in Runtime,
        target,
        version,
        streams
      ).map { (deps, cp, tgt, v, strms) =>
        val artifacts = for {
          af       <- cp
          artifact <- af.get(artifact.key)
        } yield artifact.name

        if (!(Set("coppersmith-core_2.11", "coppersmith-scalding_2.11", "coppersmith-tools_2.11") forall (artifacts contains)))
          sys.error("The Coppersmith plugin requires coppersmith-core, coppersmith-scalding and coppersmith-tools. " +
            "Please add these as dependencies in your build.sbt.")

        val classpathString = cp.files.mkString(":")
        val res = Process(
          Seq("java", "-cp", classpathString, "commbank.coppersmith.tools.MetadataMain", "--json", "")
        )!!

        if (res.length == 0) {
          error("Metadata empty. Are your feature definitions in a subproject?")
        } else {
          val metadataFile = tgt / s"coppersmith-features-${v}.json"

          IO.write(metadataFile, res.getBytes)
          strms.log.info(s"Feature metadata written to $metadataFile")

          metadataFile
        }
      }
  ))

  override def requires = plugins.JvmPlugin && plugins.IvyPlugin
  override def trigger = allRequirements

  override lazy val projectSettings = super.projectSettings ++ baseMetadataSettings
}
