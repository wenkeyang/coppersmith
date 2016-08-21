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

package commbank.coppersmith.scalding

import org.joda.time.DateTime

import com.twitter.scalding.{Config, Execution, TypedPipe}

import org.scalacheck.Arbitrary
import org.scalacheck.Gen.alphaStr
import org.scalacheck.Prop.forAll

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.test.Records

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.Fact
import au.com.cba.omnia.thermometer.fact.PathFactoids.{exists, records, lines}
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import commbank.coppersmith._, Feature._
import Arbitraries._

import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.test.thrift.Account
import commbank.coppersmith.api.scalding.EavtText.{EavtEnc, eavtByDay}

import TestFeatureSets.{RegularFeatures, AggregationFeatures}

class ScaldingJobSpec extends ThermometerHiveSpec with Records { def is = s2"""
    Running a regular feature set job
      writes all regular feature values $regularFeaturesJob ${tag("slow")}

    Running an aggregation feature set job
      writes all aggregation feature values $aggregationFeaturesJob ${tag("slow")}

    Running a multi feature set job
      writes feature values for seq sets $multiFeatureSetJobSeq ${tag("slow")}

    Running a multi feature set job
      writes feature values for par sets $multiFeatureSetJobPar ${tag("slow")}
  """

  {
    import java.util.logging.Logger
    // Suppress parquet logging (which wraps java.util.logging) in tests. Loading the parquet.Log
    // class forces its static initialiser block to be run prior to removing the root logger
    // handlers, which needs to be done to avoid the Log's default handler being re-added. Disabling
    // log output specific to the parquet logger would be more ideal, however, this has proven to be
    // non-trivial due to what appears to be a result of configuration via static initialisers and
    // the order in which the initialisers are called.
    // FIXME: This can be replaced by TestUtil.withoutLogging when slf4j is available in parquet
    // via https://github.com/apache/parquet-mr/pull/290
    Logger.getLogger(getClass.getName).info("Suppressing further java.util.logging log output")
    Class.forName("parquet.Log")
    val rootLogger = Logger.getLogger("")
    rootLogger.getHandlers.foreach(rootLogger.removeHandler)
  }

  def prepareData(
    custAccts:  CustomerAccounts,
    jobTime:    DateTime,
    sink: HiveTextSink[Eavt]
  ): FeatureJobConfig[Account] = {

    val accounts = custAccts.cas.flatMap(_.as)
    case class PrepareConfig(config: Config) {
      val maestro = MaestroConfig(config, "account", "account", "account")
      val acctTable = maestro.hiveTable[Account]("account", "account_db", Some("account_db"))
    }

    val job: Execution[JobStatus] = for {
      conf  <- Execution.getConfig.map(PrepareConfig(_))
               // Clear data from previous tests
      _     <- Execution.fromHdfs(Hdfs.delete(path(s"$dir/user/account_db"), true))
      _     <- Execution.fromHdfs(Hdfs.delete(path(s"$dir/user/features_db"), true))
      count <- viewHive(conf.acctTable, TypedPipe.from(accounts))
      _     <- Execution.guard(count == accounts.size, s"$count != ${accounts.size}")
    } yield JobFinished

    executesOk(job, Map("hdfs-root" -> List(s"$dir/user")))

    val accountDataSource =
      HiveParquetSource[Account, Nothing](path(s"$dir/user/account_db"), Partitions.unpartitioned)

    new FeatureJobConfig[Account] {
      val featureContext = ExplicitGenerationTime(jobTime)
      val featureSource  = From[Account].bind(SourceBinder.from(accountDataSource))
      val featureSink    = sink
    }
  }

  val eavtReader  = delimitedThermometerRecordReader[Eavt]('|', "\\N", implicitly[Decode[Eavt]])
  val defaultArgs = Map("hdfs-root" -> List(s"$dir/user"))
  val sink = HiveTextSink[Eavt](
    "features_db",
    path(s"$dir/user/features_db"),
    "features",
    eavtByDay
  )

  // Use alphaStr to avoid problems with serialising new lines and eavt field delimiters
  implicit val arbCustAccts: Arbitrary[CustomerAccounts] = arbCustomerAccounts(alphaStr)

  def regularFeaturesJob =
    forAll { (custAccts: CustomerAccounts, jobTime: DateTime) => {
      val cfg = prepareData(custAccts, jobTime, sink)
      val expected = RegularFeatures.expectedFeatureValues(custAccts, jobTime)

      withEnvironment(path(getClass.getResource("/").toString)) {
        executesOk(SimpleFeatureJob.generate((_: Config) => cfg, RegularFeatures), defaultArgs)
        facts(successFlagsWritten(expected, jobTime): _*)
        facts(metadataWritten(expected, List(RegularFeatures)): _*)
        facts(path(s"${sink.tablePath}/*/*/*/[^_]*") ==> records(eavtReader, expected))

      }
    }}.set(minTestsOk = 5)

  def aggregationFeaturesJob =
    forAll { (custAccts: CustomerAccounts, jobTime: DateTime) => {
      val cfg = prepareData(custAccts, jobTime, sink)
      val expected = AggregationFeatures.expectedFeatureValues(custAccts, jobTime)


      withEnvironment(path(getClass.getResource("/").toString)) {
        executesOk(SimpleFeatureJob.generate((_: Config) => cfg, AggregationFeatures), defaultArgs)
        facts(successFlagsWritten(expected, jobTime): _*)
        facts(metadataWritten(expected, List(AggregationFeatures)): _*)
        facts(path(s"${sink.tablePath}/*/*/*/[^_]*") ==> records(eavtReader, expected))
      }
    }}.set(minTestsOk = 5)

  def multiFeatureSetJobPar =
    forAll { (custAccts: CustomerAccounts, jobTime: DateTime) => {
      val cfg = prepareData(custAccts, jobTime, sink)
      val expected =
        RegularFeatures.expectedFeatureValues(custAccts, jobTime) ++
          AggregationFeatures.expectedFeatureValues(custAccts, jobTime)

      withEnvironment(path(getClass.getResource("/").toString)) {
        val job = FeatureSetExecutions(
          FeatureSetExecution((_: Config) => cfg, RegularFeatures),
          FeatureSetExecution((_: Config) => cfg, AggregationFeatures)
        )
        executesOk(SimpleFeatureJob.generate(job), defaultArgs)
        facts(successFlagsWritten(expected, jobTime): _*)
        facts(metadataWritten(expected, List(RegularFeatures, AggregationFeatures)): _*)
        facts(path(s"${sink.tablePath}/*/*/*/[^_]*") ==> records(eavtReader, expected))
      }
    }}.set(minTestsOk = 5)

  def multiFeatureSetJobSeq =
    forAll { (custAccts: CustomerAccounts, jobTime: DateTime) => {
      val cfg = prepareData(custAccts, jobTime, sink)
      val expected =
        RegularFeatures.expectedFeatureValues(custAccts, jobTime) ++
          AggregationFeatures.expectedFeatureValues(custAccts, jobTime)

      withEnvironment(path(getClass.getResource("/").toString)) {
        val job = FeatureSetExecutions(
          FeatureSetExecution((_: Config) => cfg, RegularFeatures)
        ).andThen(
          FeatureSetExecution((_: Config) => cfg, AggregationFeatures)
        )
        executesOk(SimpleFeatureJob.generate(job), defaultArgs)
        facts(successFlagsWritten(expected, jobTime): _*)
        facts(metadataWritten(expected, List(RegularFeatures, AggregationFeatures)): _*)
        facts(path(s"${sink.tablePath}/*/*/*/[^_]*") ==> records(eavtReader, expected))
      }
    }}.set(minTestsOk = 5)

  private def successFlagsWritten(expectedValues: List[Eavt], dateTime: DateTime): Seq[Fact] = {
    val partition = sink.partition.underlying
    val expectedPartitions = expectedValues.map(partition.extract(_)).toSet.toSeq
    expectedPartitions.map { case (year, month, day) =>
      path(s"${sink.tablePath}/year=$year/month=$month/day=$day/_SUCCESS") ==> exists
    }
  }

  private def metadataWritten(expectedValues: List[Eavt],
                              expectedMetadataSets: List[MetadataSet[Any]]): Seq[Fact] = {
    val partition          = sink.partition.underlying
    val expectedPartitions = expectedValues.map(partition.extract(_)).toSet.toSeq

    for {
      (year, month, day) <- expectedPartitions
      em                 <- expectedMetadataSets
      expectedMetadata   =  MetadataOutput.Json1.stringify(MetadataOutput.Json1.doOutput(List(em),
                                                           Conforms.allConforms))
    } yield {
      path(s"${sink.tablePath}/year=$year/month=$month/day=$day/_feature_metadata/_${em.name}_METADATA.V1.json") ==>
        lines(expectedMetadata.split("\n").toList)
    }
  }
}
