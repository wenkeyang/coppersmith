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

// Package deliberately commented to reflect CoppersmithBootstrap
//package commbank.coppersmith.tools

import java.io.{File, PrintStream}
import java.nio.file.Files

import org.specs2.mutable.Specification

import scala.io.Source
import scala.tools.nsc.{Settings, Global}

class CoppersmithBootstrapTest extends Specification {

  "CoppersmithBootstrap" should {
    "generate valid classes" in {
      val tempDir = Files.createTempDirectory(null)
      val infile  = new File("src/test/resources/simple_test.psv")
      val outfile = new File(tempDir.toFile, "Customer.scala")

      try {
        CoppersmithBootstrap.main(Array(
          "--source-type", "Customer",
          "--file", "src/test/resources/simple_test.psv",
          "--out", outfile.getAbsolutePath
        ))
        success("Generate class from PSV")
      } catch {
        case e: Exception => failure("Generator failed to generate class: " + e.getMessage)
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
      } catch {
        case e: Exception => failure("Load generated class: " + e.getMessage)
      }

      val tryClass2 = "Customer"
      try {
        val clazz = classLoader.loadClass(tryClass2) // load class
        success("Load generated class: " + tryClass2)
      } catch {
        case e: Exception => failure("Load generated class: " + e.getMessage)
      }
      ok
    }
  }
}
