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

import com.twitter.scalding.{Execution, TypedPipe}
import org.joda.time.DateTime

import org.scalacheck.{Arbitrary, Prop}, Arbitrary._, Prop.forAll

import scalaz.NonEmptyList

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.test.Records

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids.{exists, missing, records}
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import commbank.coppersmith._, Arbitraries._, Feature.Value
import ScaldingArbitraries._
import thrift.Eavt

class EavtSinkSpec extends ThermometerHiveSpec with Records { def is = s2"""
    Writing features to an EavtSink
      writes all feature values            $featureValuesOnDiskMatch        ${tag("slow")}
      writes multiple results              $multipleValueSetsOnDiskMatch    ${tag("slow")}
      exposes features through hive        $featureValuesInHiveMatch        ${tag("slow")}
      commits all partitions with SUCCESS  $expectedPartitionsMarkedSuccess ${tag("slow")}
      fails if sink is committed           $writeFailsIfSinkCommitted       ${tag("slow")}
      fails to commit if sink is committed $commitFailsIfSinkCommitted      ${tag("slow")}
  """

  implicit val arbConfig: Arbitrary[EavtSink.Config] =
    Arbitrary(for {
                dbName <- arbNonEmptyAlphaStr
                dbPath <- arbitrary[Path]
                tableName <- arbNonEmptyAlphaStr
              } yield EavtSink.Config(dbName.value, new Path(dir, dbPath), tableName.value))

  // Current EAVT sink implementation lacks support for encoding control characters.

  implicit val arbFeatureValues: Arbitrary[NonEmptyList[FeatureValue[Value]]] = {
    import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary
    import Feature.Value.Str
    Arbitrary(
      NonEmptyListArbitrary[FeatureValue[Value]].arbitrary.map(nel =>
        nel.map {
          case v@FeatureValue(_, _, Str(s)) =>
            v.copy(value = Str(s.map(_.filterNot(_ < 32).replace(EavtSink.Delimiter, ""))))
          case v => v
        }
      )
    )
  }

  val eavtReader = delimitedThermometerRecordReader[Eavt]('|', "\\N", implicitly[Decode[Eavt]])
  def valuePipe(vs: NonEmptyList[FeatureValue[Value]], dateTime: DateTime) =
    TypedPipe.from(vs.list.map(v => v -> dateTime.getMillis))

  def featureValuesOnDiskMatch =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], eavtConfig: EavtSink.Config, dateTime: DateTime) => {
      val expected = vs.map(EavtSink.toEavt(_, dateTime.getMillis)).list

      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = EavtSink(eavtConfig)
        executesSuccessfully(sink.write(valuePipe(vs, dateTime)))
        facts(
          path(s"${eavtConfig.hiveConfig.path}/*/*/*/*") ==> records(eavtReader, expected)
        )
      }
    }}.set(minTestsOk = 5)

  def multipleValueSetsOnDiskMatch =
    forAll { (vs1: NonEmptyList[FeatureValue[Value]],
              vs2: NonEmptyList[FeatureValue[Value]],
              eavtConfig: EavtSink.Config,
              dateTime: DateTime) => {
      val expected = (vs1.list ++ vs2.list).map(EavtSink.toEavt(_, dateTime.getMillis))

      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = EavtSink(eavtConfig)
        // Suppress spurious AlreadyExistsException logging by framework when writing in parallel
        TestUtil.withoutLogging(
          "org.apache.hadoop.hive.metastore.RetryingHMSHandler",
          "hive.ql.metadata.Hive"
        ) {
            executesSuccessfully {
              sink.write(valuePipe(vs1, dateTime)).zip(sink.write(valuePipe(vs2, dateTime)))
            }
          }

        facts(
          path(s"${eavtConfig.hiveConfig.path}/*/*/*/*") ==> records(eavtReader, expected)
        )
      }
    }}.set(minTestsOk = 5)

  def featureValuesInHiveMatch =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], eavtConfig: EavtSink.Config, dateTime: DateTime) => {
      def hiveNull(s: String) = if (s == EavtSink.NullValue) "NULL" else s
      val expected = vs.map(value => {
        val eavt = EavtSink.toEavt(value, dateTime.getMillis)
        val (year, month, day) = EavtSink.partition.extract(eavt)
        List(eavt.entity, eavt.attribute, hiveNull(eavt.value), eavt.time, year, month, day).mkString("\t")
      }).list.toSet

      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = EavtSink(eavtConfig)
        val hiveConf = eavtConfig.hiveConfig
        val query = s"""SELECT * FROM `${hiveConf.database}.${hiveConf.tablename}`"""

        executesSuccessfully(sink.write(valuePipe(vs, dateTime)))
        val actual = executesSuccessfully(Execution.fromHive(Hive.query(query)))
        actual.toSet must_== expected.toSet
      }
    }}.set(minTestsOk = 5)

  def expectedPartitionsMarkedSuccess =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], eavtConfig: EavtSink.Config, dateTime: DateTime) =>  {
      val expectedPartitions = vs.map(v =>
        EavtSink.partition.extract(EavtSink.toEavt(v, dateTime.getMillis))
      ).list.toSet.toSeq
      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = EavtSink(eavtConfig)
        val writeResult = executesSuccessfully(sink.write(valuePipe(vs, dateTime)))

        // Not yet committed; _SUCCESS should be missing
        facts(
          expectedPartitions.map { case (year, month, day) =>
            path(s"${eavtConfig.hiveConfig.path}/year=$year/month=$month/day=$day/_SUCCESS") ==> missing
          }: _*
        )

        writeResult.fold(
          e => failure("Unexpected write failure: " + e),
          paths => {
            val commitResult = executesSuccessfully(FeatureSink.commit(paths))
            facts(
              expectedPartitions.map { case (year, month, day) =>
                path(s"${eavtConfig.hiveConfig.path}/year=$year/month=$month/day=$day/_SUCCESS") ==> exists
              }: _*
            )
            commitResult must beRight
          }
        )
      }
    }}.set(minTestsOk = 5)

  def writeFailsIfSinkCommitted =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], eavtConfig: EavtSink.Config, dateTime: DateTime) =>  {
      val expected = vs.map(EavtSink.toEavt(_, dateTime.getMillis)).list
      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = EavtSink(eavtConfig)
        val writeResult = executesSuccessfully(sink.write(valuePipe(vs, dateTime)))

        writeResult.fold(
          e => failure("Unexpected write failure: " + e),
          paths => {
            executesSuccessfully(FeatureSink.commit(paths))

            val secondWriteResult = executesSuccessfully(sink.write(valuePipe(vs, dateTime)))

            // Make sure no duplicate records writen
            facts(
              path(s"${eavtConfig.hiveConfig.path}/*/*/*/*") ==> records(eavtReader, expected)
            )
            secondWriteResult must beLeft.like {
              case FeatureSink.AttemptedWriteToCommitted(_) => true
            }
          }
        )
      }
    }}.set(minTestsOk = 5)

  def commitFailsIfSinkCommitted =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], eavtConfig: EavtSink.Config, dateTime: DateTime) =>  {
      val expected = vs.map(EavtSink.toEavt(_, dateTime.getMillis)).list
      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = EavtSink(eavtConfig)
        val writeResult = executesSuccessfully(sink.write(valuePipe(vs, dateTime)))

        writeResult.fold(
          e => failure("Unexpected write failure: " + e),
          paths => {
            executesSuccessfully(FeatureSink.commit(paths))
            val secondCommitResult = executesSuccessfully(FeatureSink.commit(paths))

            secondCommitResult must beLeft.like { case FeatureSink.AlreadyCommitted(_) => true }
          }
        )
      }
    }}.set(minTestsOk = 5)
}
