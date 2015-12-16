import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin.depend.versions
import sbt._
import sbt.Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

object build extends Build {
  val maestroVersion = "2.16.2-20151203053255-2ec72cc"
  val thermometerVersion = "1.3.0-20151122230202-55282c8"

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformPublicDependencySettings ++
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
    , aggregate = Seq(core, testProject, examples, scalding, tools)
  )

  lazy val core = Project(
    id = "core"
    , base = file("core")
    , settings =
      standardSettings
   ++ uniform.project("coppersmith-core", "commbank.coppersmith")
   ++ uniformThriftSettings
   ++ Seq(
          libraryDependencies += "io.github.lukehutch" % "fast-classpath-scanner" % "1.9.7",
          libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % versions.specs % "test"
            exclude("org.scala-lang", "scala-compiler"),
          libraryDependencies ++= depend.testing(),
          libraryDependencies ++= depend.omnia("maestro", maestroVersion),
          parallelExecution in Test := false
      )
  ).configs( IntegrationTest )

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
        libraryDependencies ++= depend.omnia("thermometer-hive", thermometerVersion),
        sourceGenerators in Compile <+= (sourceManaged in Compile, streams) map { (outdir: File, s) =>
          val infile = "USERGUIDE.markdown"
          val source = io.Source.fromFile(infile)
          val fileContent = try source.mkString finally source.close()
          val sourceCode = """```scala(?s)(.*?)```""".r
          val codeFragments = (sourceCode findAllIn fileContent).matchData.map {
            _.group(1)
          }
          codeFragments.zipWithIndex.map { case (frag, i) =>
            val newFile = outdir / s"userGuideFragment$i.scala"
            IO.write(newFile, frag)
            newFile
          }.toSeq
        }
      )
  ).dependsOn(core, scalding, testProject)

  lazy val testProject = Project(
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

  val sbtCPTask = taskKey[Unit]("tools/test:sbtCPTask")

  lazy val tools2 = Project(
    id = "tools2"
    , base = file("tools2")
    , settings =
      Defaults.coreDefaultSettings
        ++ uniformDependencySettings
        ++ uniform.project("coppersmith-tools", "commbank.coppersmith.tools")
        ++ Seq(fork in Test := true)
        ++ Seq(libraryDependencies ++= Seq(
        "org.specs2" %% "specs2-matcher-extra" % versions.specs % "test")
      )
  ).dependsOn(core)

  lazy val tools = Project(
    id = "tools"
    , base = file("tools")
    , settings =
      Defaults.coreDefaultSettings
        ++ uniformDependencySettings
        ++ uniform.project("coppersmith-tools", "commbank.coppersmith.tools")
        ++ Seq(libraryDependencies ++= Seq(
        "org.specs2" %% "specs2-matcher-extra" % versions.specs % "test")
      )
        ++ Seq(sbtCPTask := {
            val files: Seq[File] = (fullClasspath in Compile).value.files
            val sbtClasspath: String = files.map(x => x.getAbsolutePath).mkString(":")
            println("Set SBT classpath to 'sbt-classpath' environment variable")
            System.setProperty("sbt-classpath", sbtClasspath)
          })
        ++ Seq((testExecution in test in Test) <<= (testExecution in test in Test) dependsOn (sbtCPTask))
        ++ Seq((testExecution in testOnly in Test) <<= (testExecution in test in Test) dependsOn (sbtCPTask))
        ++ Seq(resourceGenerators in Compile <+= (resourceManaged in Compile, streams) map {
        (outdir: File, s) =>
          val infile = "tools/src/main/bash/CoppersmithBootstrap.sh"
          val infile2 = "tools/src/main/scala/CoppersmithBootstrap.scala"
          val outfile = outdir / "CoppersmithBootstrap.sh"
          outfile.getParentFile.mkdirs
          s"cat ${infile} ${infile2}" #> outfile !! s.log
          Seq(outfile)
      })
  ).dependsOn(core)
}
