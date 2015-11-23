import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin.depend.versions
import sbt._
import sbt.Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

object build extends Build {
  val maestroVersion = "2.16.0-20151123035255-1a0be7f"

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
          libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % versions.specs % "test"
            exclude("org.scala-lang", "scala-compiler"),
          libraryDependencies ++= depend.testing(),
          libraryDependencies ++= depend.omnia("maestro", maestroVersion),
          parallelExecution in Test := false
      )
  )

  lazy val scalding = Project(
    id = "scalding"
    , base = file("scalding")
    , settings =
      standardSettings
        ++ uniform.project("coppersmith-scalding", "commbank.coppersmith.scalding")
        ++ uniformThriftSettings
        ++ Seq(
        libraryDependencies ++= depend.hadoopClasspath,
        libraryDependencies ++= depend.omnia("maestro-test", maestroVersion, "test"),
        libraryDependencies ++= depend.parquet(),
        parallelExecution in Test := false
      )
  ).dependsOn(core % "compile->compile;test->test")

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
         libraryDependencies ++= depend.hadoopClasspath,
         sourceGenerators in Compile <+= (sourceManaged in Compile, streams) map { (outdir: File, s) =>
           val infile  = "USERGUIDE.markdown"
           val source = io.Source.fromFile(infile)
           val fileContent = try source.mkString finally source.close()
           val sourceCode = """```scala(?s)(.*?)```""".r
           val codeFragments = (sourceCode findAllIn fileContent).matchData.map {_.group(1)}
           codeFragments.zipWithIndex.map { case (frag, i) =>
              val newFile = outdir / s"userGuideFragment$i.scala"
              IO.write(newFile, frag)
              newFile
           }.toSeq
         }
       )
  ).dependsOn(core, scalding, test)

  lazy val test = Project(
    id = "test"
  , base = file("test")
  , settings =
      standardSettings
   ++ uniform.project("coppersmith-test", "commbank.coppersmith.test")
   ++ uniformThriftSettings
   ++ Seq(
        libraryDependencies ++= depend.testing()
      )

  ).dependsOn(core)
}
