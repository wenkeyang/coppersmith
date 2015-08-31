import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin.depend.versions
import sbt._
import sbt.Keys._

import au.com.cba.omnia.humbug.HumbugSBT.humbugSettings

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

object build extends Build {
  val humbugVersion  = "0.6.1-20150513010955-5eb6297"
  val maestroVersion = "2.13.1-20150728061651-8d9c378"

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformDependencySettings ++
    strictDependencySettings ++
    Seq(
      scalacOptions += "-Xfatal-warnings",
      scalacOptions in (Compile, console) ~= (_.filterNot(Set("-Xfatal-warnings", "-Ywarn-unused-import"))),
      scalacOptions in (Compile, doc) ~= (_ filterNot (_ == "-Xfatal-warnings")),
      scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
    )

  lazy val all = Project(
    id = "all"
  , base = file(".")
  , settings =
      standardSettings
   ++ uniform.project("coppersmith-all", "au.com.cba.omnia.dataproducts.features.all")
   ++ Seq(
        publishArtifact := false
      )
  , aggregate = Seq(core, test, examples)
  )

  lazy val core = Project(
    id = "core"
  , base = file("core")
  , settings =
      standardSettings
   ++ uniform.project("coppersmith", "au.com.cba.omnia.dataproducts.features")
   ++ uniformThriftSettings
   ++ Seq(
          libraryDependencies ++= depend.hadoopClasspath,
          libraryDependencies ++= depend.omnia("maestro", maestroVersion),
          libraryDependencies ++= depend.omnia("maestro-test", maestroVersion, "test"),
          libraryDependencies ++= depend.parquet(),
          libraryDependencies ++= Seq(
             "org.specs2" %% "specs2-matcher-extra" % versions.specs
          ) ++  depend.testing()
        , parallelExecution in Test := false
      )
  )

  lazy val examples = Project(
    id = "examples"
  , base = file("examples")
  , settings =
      standardSettings
    ++ uniform.project("coppersmith-examples", "au.com.omnia.dataproducts.features.examples")
    ++ uniformAssemblySettings
    ++ Seq(
        libraryDependencies ++= depend.scalding(),
        libraryDependencies ++= depend.hadoopClasspath,
        libraryDependencies ++= depend.scalding(),
        libraryDependencies ++= depend.omnia("humbug-core", humbugVersion)
      , sourceGenerators in Compile <+= (sourceManaged in Compile, streams) map { (outdir: File, s) =>
          // Poor man's "Literate Scala". (Consider alternatives such as https://github.com/non/literati)
          // This is part of the "examples" project because it depends on a thrift spec there.
          val outfile = outdir / "USERGUIDE.scala"
          file("USERGUIDE.markdown") #> "sed -n /```scala/,/```/p" #| "grep -v ```" #> outfile ! s.log
          Seq(outfile)
        }
      )
    ++ humbugSettings
  ).dependsOn(core)

  lazy val test = Project(
    id = "test"
  , base = file("test")
  , settings =
      standardSettings
   ++ uniform.project("coppersmith-test", "au.com.cba.omnia.dataproducts.features.test")
   ++ uniformThriftSettings
   ++ Seq(
        libraryDependencies ++= depend.testing()
      )

  ).dependsOn(core)
}
