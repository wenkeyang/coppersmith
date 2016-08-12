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

import commbank.coppersmith.api._, Coppersmith._, scalding.SimpleFeatureJob
import commbank.coppersmith.examples.thrift.{Movie, Rating}

import UserGuideArbitraries.{arbMovie, arbRating}

object JoinFeaturesSpec extends ThermometerHiveSpec { def is = s2"""
(these two jobs are different implementations of the same logic, and should behave identically)
  JoinFeaturesJob must return expected values        $testV1  ${tag("slow")}
  ComedyJoinFeaturesJob must return expected values  $testV2  ${tag("slow")}
"""
  def testV1 = test(JoinFeaturesJob, "COMEDY_MOVIE_AVG_RATING")
  def testV2 = test(ComedyJoinFeaturesJob, "COMEDY_MOVIE_AVG_RATING_V2")

  def test(job: SimpleFeatureJob, featureName: String) = {
    def movie(id: String, isComedy: Int) =
      arbitrary[Movie].sample.get.copy(id = id, comedy = isComedy)

    def rating(movie: String, rating: Int) =
      arbitrary[Rating].sample.get.copy(movieId = movie, rating = rating)

    writeRecords[Movie](s"$dir/user/data/movies/data.txt", "|", Seq(
      movie("1", 0),  // non-comedy
      movie("2", 1)    // comedy
    ))

    writeRecords[Rating](s"$dir/user/data/ratings/data.txt", "\t", Seq(
      rating("1", 3),
      rating("2", 2),
      rating("2", 5)
    ))

    executesSuccessfully(job.job) must_== JobFinished

    val outPath = s"$dir/user/dev/ratings/year=2015/month=01/day=01/part-*"
    expectations { context =>
      context.lines(new Path(outPath)).toSet must_==
        Set(s"2|$featureName|3.5|2015-01-01")
    }
  }

  def writeRecords[T : Encode](path: String, delim: String, records: Seq[T]): Unit = {
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
