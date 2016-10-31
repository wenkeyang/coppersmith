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
import org.scalacheck.{Arbitrary, Gen}, Arbitrary._

import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import commbank.coppersmith.examples.thrift.Rating
import commbank.coppersmith.api._, Coppersmith._
import UserGuideArbitraries.arbRating

object RatingFeaturesSpec extends ThermometerHiveSpec { def is = s2"""
  RatingFeaturesJob must return expected values  $test  ${tag("slow")}
"""
  def test = {
    // Override the default implicit Arbitrary[String] (brought into scope by Arbitrary.arbString)
    // to avoid generating Customer & Account records with strings that can't be safely written to
    // a Hive Text store (due to newline or field separator characters being generated).
    implicit def arbSafeHiveTextString: Arbitrary[String] = Arbitrary(Gen.identifier)

    def rating(movie: String, rating: Int) =
      arbitrary[Rating].sample.get.copy(movieId = movie, rating = rating)

    writeRecords[Rating](s"$dir/user/data/ratings/data.txt", Seq(
      rating("1", 3),
      rating("2", 2),
      rating("2", 5)
    ), "\t")

    executesSuccessfully(RatingFeaturesJob.job) must_== JobFinished

    val outPath = s"$dir/user/dev/ratings/year=2015/month=01/day=01/part-*"
    expectations { context =>
      context.lines(new Path(outPath)).toSet must_==
        Set(
          "1|MOVIE_AVG_RATING|3.0|2015-01-01",
          "1|RATING_STANDARD_DEVIATION|NaN|2015-01-01",
          "2|MOVIE_AVG_RATING|3.5|2015-01-01",
          "2|RATING_STANDARD_DEVIATION|1.5|2015-01-01"
        )
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
