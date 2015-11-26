import java.io.File
import java.nio.file.Files

import org.specs2.mutable.Specification

import scala.io.Source
import scala.tools.nsc.{Settings, Global}

class CoppersmithBootstrapTest extends Specification {

  "CoppersmithBootstrap" should {
    "generate valid classes" in {
      val tempDir = Files.createTempDirectory(null)
      val infile  = "tools/src/test/resources/simple_test.psv"
      val outfile  = new File(tempDir.toFile, "Customer.scala")
      val cmdArgs: Array[String] = Array("--source-type", "Customer", "--file", s"$infile", "--out", s"${outfile.getAbsolutePath}")

      try {
        CoppersmithBootstrap.main(cmdArgs)
        success("Generate class from PSV")
      }
      catch {
        case t: Throwable => failure("Generator failed to generate class: " + t.getMessage)
      }

      val s = new Settings()
      s.outputDirs.add(tempDir.toFile.getAbsolutePath, tempDir.toFile.getAbsolutePath)
      s.classpath.value = tempDir.toFile.getAbsolutePath +":"+System.getProperty("sbt-classpath")

      val g = new Global(s)
      val run = new g.Run

      run.compile(List(outfile.getAbsolutePath))

      val classLoader = new java.net.URLClassLoader(
        Array(tempDir.toUri.toURL),  // Using temp directory.
        this.getClass.getClassLoader)

      val tryClass = "CustomerFeatureSet"
      try {

            val clazz = classLoader.loadClass(tryClass) // load class
            success("Load generated class: " + tryClass)
      }
      catch {
        case t: Throwable => failure("Load generated class: " + t.getMessage)
      }

      val tryClass2 = "Customer"
      try {

        val clazz = classLoader.loadClass(tryClass2) // load class
        success("Load generated class: " + tryClass2)
      }
      catch {
        case t: Throwable => failure("Load generated class: " + t.getMessage)
      }
      ok
    }
  }
}
