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

import com.twitter.scalding.Execution

import org.apache.hadoop.fs.Path

import org.scalacheck.Prop.forAll

import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec
import au.com.cba.omnia.thermometer.core.Thermometer.path
import au.com.cba.omnia.maestro.api._, Maestro._

import commbank.coppersmith.api.JobFinished
import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.User

import UserGuideArbitraries.arbUser

object DistinctFeaturesSpec extends ThermometerHiveSpec { def is = s2"""
  DistinctFeaturesJob must return expected values  $test  ${tag("slow")}
"""
  def test = forAll { (users: Seq[User]) => {
    writeRecords[User](s"$dir/user/data/users/data.txt", users, "|")
    val expectedLength = users.groupBy(_.id).keys.size

    val job = for {
      // Clear previous test data
      _      <- Execution.fromHdfs(Hdfs.delete(path(s"$dir/user/dev/users"), true))
      result <- DistinctUserFeaturesJob.job
    } yield result

    executesSuccessfully(job) must_== JobFinished

    val outPath = s"$dir/user/dev/users/year=2015/month=01/day=01/*"
    expectations { context =>
      context.lines(new Path(outPath)).toSet.size must_== expectedLength
    }
  }}.set(minTestsOk = 5)

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
