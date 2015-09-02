import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin.depend.versions
import sbt._
import sbt.Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

object build extends Build {
  val maestroVersion = "2.13.1-20150728061651-8d9c378"

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformDependencySettings ++
//    strictDependencySettings ++
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
   ++ uniform.project("coppersmith-all", "commbank.coppersmith.all")
   ++ Seq(
        publishArtifact := false
      )
  , aggregate = Seq(core, test, examples, scalding)
  )

  lazy val core = Project(
    id = "core"
  , base = file("core")
  , settings =
      standardSettings
   ++ uniform.project("coppersmith-core", "commbank.coppersmith")
   ++ uniformThriftSettings
   ++ Seq(
          dependencyOverrides +=   "org.specs2" %% "specs2-core"  % "3.6.3"   ,
          libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % "3.6.4",
          libraryDependencies ++= depend.testing(specs = "3.6.4"),
          libraryDependencies += "org.specs2" %% "specs2-junit"  % "3.6.3",

          libraryDependencies ++= depend.omnia("maestro", maestroVersion),
          parallelExecution in Test := false,
        testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "junitxml", "console")
      )
  ).disablePlugins (plugins.JUnitXmlReportPlugin)

  lazy val scalding = Project(
    id = "scalding"
    , base = file("scalding")
    , settings =
      standardSettings
        ++ uniform.project("coppersmith-scalding", "commbank.coppersmith.scalding")
        ++ uniformThriftSettings
        ++ Seq(
        libraryDependencies ++= depend.hadoopClasspath,
        libraryDependencies ++= depend.omnia("maestro", maestroVersion),
        libraryDependencies ++= depend.omnia("maestro-test", maestroVersion, "test"),
        libraryDependencies ++= depend.parquet(),
        libraryDependencies += "org.specs2" %% "specs2-junit"  % "3.6.3",
        dependencyOverrides +=   "org.specs2" %% "specs2-core"  % "3.6.3",
        libraryDependencies ++= Seq(
          "org.specs2" %% "specs2-matcher-extra" % "3.6.4"
        ) ++  depend.testing(specs = "3.6.4")
        , parallelExecution in Test := false,
        testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "junitxml", "console")
      )
  ).dependsOn(core).disablePlugins (plugins.JUnitXmlReportPlugin)

  lazy val examples = Project(
    id = "examples"
  , base = file("examples")
  , settings =
       standardSettings
    ++ uniform.project("coppersmith-examples", "commbank.coppersmith.examples")
    ++ uniformThriftSettings
    ++ uniformAssemblySettings
    ++ Seq(
         libraryDependencies ++= depend.scalding(),
         libraryDependencies ++= depend.hadoopClasspath
       )
  ).dependsOn(core, scalding)

  lazy val test = Project(
    id = "test"
  , base = file("test")
  , settings =
      standardSettings
   ++ uniform.project("coppersmith-test", "commbank.coppersmith.test")
   ++ uniformThriftSettings
   ++ Seq(
        libraryDependencies ++= depend.testing(specs = "3.6.4")
      )

  ).dependsOn(core)
}
