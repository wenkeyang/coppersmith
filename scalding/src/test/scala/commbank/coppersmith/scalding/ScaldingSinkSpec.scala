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

import au.com.cba.omnia.thermometer.core.{Thermometer, ThermometerRecordReader}, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids.{exists, missing, records}
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import commbank.coppersmith._, Arbitraries._, Feature.Value
import ScaldingArbitraries.arbHivePath
import thrift.Eavt

abstract class ScaldingSinkSpec[T <: FeatureSink] extends ThermometerHiveSpec with Records { def is = s2"""
    Writing features to an EavtSink
      writes all feature values            $featureValuesOnDiskMatch        ${tag("slow")}
      writes multiple results              $multipleValueSetsOnDiskMatch    ${tag("slow")}
      exposes features through hive        $featureValuesInHiveMatch        ${tag("slow")}
      commits all partitions with SUCCESS  $expectedPartitionsMarkedSuccess ${tag("slow")}
      fails if sink is committed           $writeFailsIfSinkCommitted       ${tag("slow")}
      fails to commit if sink is committed $commitFailsIfSinkCommitted      ${tag("slow")}
  """

  type SinkAndTime = (T, DateTime)
  implicit def arbSinkAndTime:   Arbitrary[SinkAndTime]
  implicit def arbFeatureValues: Arbitrary[NonEmptyList[FeatureValue[Value]]]

  implicit def eavtEnc: FeatureValueEnc[Eavt]

  def eavtReader: ThermometerRecordReader[Eavt]

  def tablePath(t: T):    String
  def databaseName(t: T): String
  def tableName(t: T):    String

  def valuePipe(vs: NonEmptyList[FeatureValue[Value]], dateTime: DateTime) =
    TypedPipe.from(vs.list.map(v => v -> dateTime.getMillis))

  def featureValuesOnDiskMatch =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], sinkAndTime: SinkAndTime) => {
      val (sink, dateTime) = sinkAndTime
      val expected = vs.map(v => eavtEnc.encode((v, dateTime.getMillis))).list

      withEnvironment(path(getClass.getResource("/").toString)) {
        executesSuccessfully(sink.write(valuePipe(vs, dateTime)))
        facts(
          path(s"${tablePath(sink)}/*/*/*/*") ==> records(eavtReader, expected)
        )
      }
    }}.set(minTestsOk = 5)

  def multipleValueSetsOnDiskMatch =
    forAll { (vs1: NonEmptyList[FeatureValue[Value]],
              vs2: NonEmptyList[FeatureValue[Value]],
              sinkAndTime: SinkAndTime) => {
      val (sink, dateTime) = sinkAndTime
      val expected = (vs1.list ++ vs2.list).map(v => eavtEnc.encode((v, dateTime.getMillis)))

      withEnvironment(path(getClass.getResource("/").toString)) {
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
          path(s"${tablePath(sink)}/*/*/*/*") ==> records(eavtReader, expected)
        )
      }
    }}.set(minTestsOk = 5)

  def featureValuesInHiveMatch =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], sinkAndTime: SinkAndTime) => {
      val (sink, dateTime) = sinkAndTime
      def hiveNull(s: String) = if (s == HiveTextSink.NullValue) "NULL" else s
      val expected = vs.map(value => {
        val eavt = eavtEnc.encode((value, dateTime.getMillis))
        val (year, month, day) = FixedSinkPartition.byDay(dateTime).partitionValue
        List(eavt.entity, eavt.attribute, hiveNull(eavt.value), eavt.time, year, month, day).mkString("\t")
      }).list.toSet

      withEnvironment(path(getClass.getResource("/").toString)) {
        val query = s"""SELECT * FROM `${databaseName(sink)}.${tableName(sink)}`"""

        executesSuccessfully(sink.write(valuePipe(vs, dateTime)))
        val actual = executesSuccessfully(Execution.fromHive(Hive.query(query)))
        actual.toSet must_== expected.toSet
      }
    }}.set(minTestsOk = 5)

  def expectedPartitionsMarkedSuccess =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], sinkAndTime: SinkAndTime) =>  {
      val (sink, dateTime) = sinkAndTime
      val (year, month, day) = FixedSinkPartition.byDay(dateTime).partitionValue
      withEnvironment(path(getClass.getResource("/").toString)) {
        val writeResult = executesSuccessfully(sink.write(valuePipe(vs, dateTime)))

        // Not yet committed; _SUCCESS should be missing
        facts(
          path(s"${tablePath(sink)}/year=$year/month=$month/day=$day/_SUCCESS") ==> missing
        )

        writeResult.fold(
          e => failure("Unexpected write failure: " + e),
          paths => {
            val commitResult = executesSuccessfully(FeatureSink.commit(paths))
            facts(
              path(s"${tablePath(sink)}/year=$year/month=$month/day=$day/_SUCCESS") ==> exists
            )
            commitResult must beRight
          }
        )
      }
    }}.set(minTestsOk = 5)

  def writeFailsIfSinkCommitted =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], sinkAndTime: SinkAndTime) =>  {
      val (sink, dateTime) = sinkAndTime
      val expected = vs.map(v => eavtEnc.encode((v, dateTime.getMillis))).list
      withEnvironment(path(getClass.getResource("/").toString)) {
        val writeResult = executesSuccessfully(sink.write(valuePipe(vs, dateTime)))

        writeResult.fold(
          e => failure("Unexpected write failure: " + e),
          paths => {
            executesSuccessfully(FeatureSink.commit(paths))

            val secondWriteResult = executesSuccessfully(sink.write(valuePipe(vs, dateTime)))

            // Make sure no duplicate records writen
            facts(
              path(s"${tablePath(sink)}/*/*/*/part-*") ==> records(eavtReader, expected)
            )

            secondWriteResult must beLeft.like {
              case FeatureSink.AttemptedWriteToCommitted(_) => true
            }
          }
        )
      }
    }}.set(minTestsOk = 5)

  def commitFailsIfSinkCommitted =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], sinkAndTime: SinkAndTime) =>  {
      val (sink, dateTime) = sinkAndTime
      val expected = vs.map(v => eavtEnc.encode((v, dateTime.getMillis))).list
      withEnvironment(path(getClass.getResource("/").toString)) {
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

class HiveTextSinkSpec extends ScaldingSinkSpec[HiveTextSink[Eavt]] {
  // HiveTextSink implementation lacks support for encoding control characters
  implicit def arbFeatureValues: Arbitrary[NonEmptyList[FeatureValue[Value]]] = {
    import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary
    import Feature.Value.Str
    Arbitrary(
      NonEmptyListArbitrary[FeatureValue[Value]].arbitrary.map(nel =>
        nel.map {
          case v@FeatureValue(_, _, Str(s)) =>
            v.copy(value = Str(s.map(_.filterNot(_ < 32).replace(HiveTextSink.Delimiter, ""))))
          case v => v
        }
      )
    )
  }

  import commbank.coppersmith.api.scalding.EavtText

  implicit def arbSinkAndTime =
    Arbitrary(
      for {
        dbName    <- hiveIdentifierGen
        tablePath <- arbitrary[Path]
        tableName <- hiveIdentifierGen
        date      <- arbLocalDate.arbitrary
        dateTime   = date.toDateTimeAtStartOfDay
      } yield {
        val sink = HiveTextSink[Eavt](
          dbName,
          new Path(dir, tablePath),
          tableName,
          EavtText.eavtByDay
        )
        (sink, dateTime)
      }
    )

  implicit def eavtEnc = EavtText.EavtEnc
  def eavtReader = delimitedThermometerRecordReader[Eavt]('|', "\\N", implicitly[Decode[Eavt]])
  def tablePath(sink: HiveTextSink[Eavt]) = sink.tablePath.toString
  def databaseName(sink: HiveTextSink[Eavt]) = sink.dbName
  def tableName(sink: HiveTextSink[Eavt]) = sink.tableName
}

class HiveParquetSinkSpec extends ScaldingSinkSpec[HiveParquetSink[Eavt, (String, String, String)]] {
  implicit def arbFeatureValues: Arbitrary[NonEmptyList[FeatureValue[Value]]] = {
    import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary
    Arbitrary(NonEmptyListArbitrary[FeatureValue[Value]].arbitrary)
  }

  implicit def arbSinkAndTime =
    Arbitrary(
      for {
        dbName    <- hiveIdentifierGen
        tablePath <- arbitrary[Path]
        tableName <- hiveIdentifierGen
        date      <- arbLocalDate.arbitrary
        dateTime   = date.toDateTimeAtStartOfDay
      } yield {
        val sink = HiveParquetSink[Eavt, (String, String, String)](
          dbName,
          tableName,
          new Path(dir, tablePath),
          FixedSinkPartition.byDay[Eavt](dateTime)
        )(implicitly, eavtEnc, implicitly, implicitly, implicitly)
        (sink, dateTime)
      }
    )

  implicit def eavtEnc = new FeatureValueEnc[Eavt] {
    import Value._
    def encode(fvt: (FeatureValue[_], Long)): Eavt = fvt match {
      case (fv, time) =>
        val featureValue = (fv.value match {
          case Integral(v) => v.map(_.toString)
          case Decimal(v) => v.map(_.toString)
          case FloatingPoint(v) => v.map(_.toString)
          case Str(v) => v
          case Date(v) => v.map(_.toString)
          case Time(v) => v.map(_.toString)
        }).getOrElse("NULL")

        val featureTime = new DateTime(time).toString("yyyy-MM-dd")
        Eavt(fv.entity, fv.name, featureValue, featureTime)
    }
  }

  import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader
  def eavtReader = ParquetThermometerRecordReader[Eavt]
  def tablePath(sink: HiveParquetSink[Eavt, (String, String, String)]) = sink.table.tablePath.toString
  def databaseName(sink: HiveParquetSink[Eavt, (String, String, String)]) = sink.table.database
  def tableName(sink: HiveParquetSink[Eavt, (String, String, String)]) = sink.table.table
}
