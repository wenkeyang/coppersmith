import sbt._
import sbt.Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._

object build extends Build {
  val etlUtilVersion = "1.14.1-20150703071108-fb2a434"

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformDependencySettings ++
    strictDependencySettings

  lazy val all = Project(
    id = "all"
  , base = file(".")
  , settings =
      standardSettings
   ++ uniform.project("features-all", "au.com.cba.omnia.dataproducts.features.all")
   ++ Seq(
        publishArtifact := false
      )
  , aggregate = Seq(core, test)
  )

  lazy val core = Project(
    id = "core"
  , base = file("core")
  , settings =
      standardSettings
   ++ uniform.project("features", "au.com.cba.omnia.dataproducts.features")
   ++ uniformThriftSettings
   ++ Seq(
        libraryDependencies ++= depend.omnia("etl-util", etlUtilVersion)
          ++ Seq(
             "au.com.cba.omnia" %% "etl-test" % etlUtilVersion % "test"
           )
        , parallelExecution in Test := false
      )
  )

  lazy val examples = Project(
    id = "examples"
  , base = file("examples")
  , settings =
      standardSettings
    ++ uniform.project("features-examples", "au.com.omnia.dataproducts.features.examples")
    ++ uniformThriftSettings
    ++ Seq(
        libraryDependencies ++=  depend.omnia("etl-util", etlUtilVersion),
        libraryDependencies ++= depend.hadoop()
      )
  ).dependsOn(core)

  lazy val test = Project(
    id = "test"
  , base = file("test")
  , settings =
      standardSettings
   ++ uniform.project("features-test", "au.com.cba.omnia.dataproducts.features.test")
   ++ uniformThriftSettings
   ++ Seq(
        libraryDependencies := depend.omnia("etl-test", etlUtilVersion)
      )
  ).dependsOn(core)
}
