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

package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import org.scalacheck.Arbitrary.arbitrary

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

import UserGuideArbitraries.arbMovie

object DistinctFeaturesSpec extends ThermometerHiveSpec { def is = s2"""
  DistinctFeaturesJob must return expected values  $test  ${tag("slow")}
"""
  def test = {
    def movie(movieId: String, releaseDate: String) =
      arbitrary[Movie].sample.get.copy(id = movieId, releaseDate = releaseDate, comedy=0)

    writeRecords[Movie](s"$dir/user/data/movies/data.txt", Seq(
      movie("1", "01-Jan-1991"),
      movie("2", "01-Jan-1993"),
      movie("2", "01-Jan-1994")
    ), "|")

    executesSuccessfully(DistinctMovieFeaturesJob.job) must_== JobFinished

    val outPath = s"$dir/user/dev/ratings/year=2015/month=01/day=01/*"
    expectations { context =>
      context.lines(new Path(outPath)).toSet.size must_== 2
    }
  }

  def writeRecords[T : Encode](path: String, records: Seq[T], delim: String): Unit = {
    val lines = records.map(t => Encode.encode("", t).mkString(delim))
    writeLines(path, lines)
  }

  def writeLines(path: String, lines: Seq[String]): Unit = {
    import java.io.{File, PrintWriter}

    val file = new File(path)
    file.getParentFile.mkdirs()
    val writer = new PrintWriter(file)
    try {
      lines.foreach { writer.println(_) }
    }
    finally {
      writer.close()
    }
  }
}
