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

package commbank.coppersmith.tools.json

import scala.util.Try

import org.specs2._, specification.core._

import argonaut._, Argonaut._



//Parses the json spec examples file to ensure that all examples can be parsed.

object ExamplesParsingSpec extends Specification {

  def verifyFragment(version: Int, json: String) = {
    json.parse.fold({err => sys.error(err)}, { parsed =>
      version match {
        case 0 =>
          val converted  = MetadataJsonV0.read(parsed).getOrElse(sys.error("Value expected"))
          val written = MetadataJsonV0.write(converted)
          Seq(
            Some(converted) must be_==(MetadataJson.read(parsed)),
            MetadataJsonV0.write(converted) must be_==(parsed),
            MetadataJson.readVersion(written) must be_== (None)
          )

        case 1 =>
          val converted  = MetadataJsonV1.read(parsed).getOrElse(sys.error("Value expected"))
          val written = MetadataJsonV1.write(converted)
          Seq(
            Some(converted) must be_==(MetadataJson.read(parsed)),
            MetadataJsonV1.write(converted) must be_==(parsed),
            MetadataJson.readVersion(written) must be_== (Some(1))
          )

        case n => sys.error(s"Unknown version $n")
      }
    })
  }


  lazy val frags = {
    val source = scala.io.Source.fromURL(
        getClass.getResource("/METADATA_JSON.markdown"))
    val markdownFile = try source.mkString finally source.close

    val versionsWithJsons: List[ (Int, List[String])] =
      extractVersionFragments(markdownFile).map { case (version, section) =>
      (version, extractJson(section))
    }

    makeTestFragments(versionsWithJsons)
  }

  def makeTestFragment(versionWithJson: (Int, List[String])): Fragments = {
    val (version, jsons) = versionWithJson
    val description = s"JSON version $version"
    val allFrags = List(
        fragmentFactory.text(description),
        fragmentFactory.break,
        fragmentFactory.tab
      ) ++ jsons.zipWithIndex.map { case ( json, i) =>
        fragmentFactory.example(s"Example $i", verifyFragment(version, json))
      } ++ List(fragmentFactory.break, fragmentFactory.backtab)
      Fragments(allFrags:_*)
    }

  def makeTestFragments(versionsWithJsons: List[(Int, List[String])]) =
    Fragments.foreach(versionsWithJsons)(makeTestFragment)

  val versionSectionsSplitRegex = "(?m)^---".r
  val versionTitleRegex = "Version (\\d+)".r
  private def extractVersionFragments(markdownFile: String): List[(Int, String)] = {
    val markdownFileSections = versionSectionsSplitRegex.split(markdownFile).toList
    markdownFileSections.flatMap { section =>
      for {
        mtch <- versionTitleRegex.findFirstMatchIn(section)
        group = mtch.group(1)
        num <- Try(group.toInt).toOption
      } yield (num, section)
    }
  }

  private def extractJson(markdown: String): List[String] = {
    val sourceCode = """```json(?s)(.*?)```""".r
    (sourceCode findAllIn markdown).matchData.map {
      _.group(1)
    }.toList
  }

  def is = SpecStructure(
    SpecHeader(
      specClass = ExamplesParsingSpec.getClass,
      title = Some("Examples from documentation should parse correctly\n"))).setFragments(frags)
}
