package commbank.coppersmith.scalding

import com.twitter.scalding.{Execution, TypedPipe}
import org.joda.time.DateTime

import org.scalacheck.{Arbitrary, Prop}, Arbitrary._, Prop.forAll

import scalaz.NonEmptyList

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.test.Records

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids.{exists, records}
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import commbank.coppersmith._, Arbitraries._, Feature.Value
import ScaldingArbitraries._
import thrift.Eavt

class HydroSinkSpec extends ThermometerHiveSpec with Records { def is = s2"""
    Writing features to a HydroSink
      writes all feature values          $featureValuesOnDiskMatch        ${tag("slow")}
      writes multiple results            $multipleValueSetsOnDiskMatch    ${tag("slow")}
      exposes features through hive      $featureValuesInHiveMatch        ${tag("slow")}
      writes all partitions with SUCCESS $expectedPartitionsMarkedSuccess ${tag("slow")}
  """

  implicit val arbConfig: Arbitrary[HydroSink.Config] =
    Arbitrary(for {
                dbName <- arbNonEmptyAlphaStr
                dbPath <- arbitrary[Path]
                tableName <- arbNonEmptyAlphaStr
              } yield HydroSink.Config(dbName.value, new Path(dir, dbPath), tableName.value))

  // Current hydro sink implementation lacks support for encoding control characters.
  // Filter them out of strings for now until Hydro switches to Parquet.
  implicit val arbFeatureValues: Arbitrary[NonEmptyList[FeatureValue[Value]]] = {
    import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary
    import Feature.Value.Str
    Arbitrary(
      NonEmptyListArbitrary[FeatureValue[Value]].arbitrary.map(nel =>
        nel.map {
          case v@FeatureValue(_, _, Str(s)) =>
            v.copy(value = Str(s.map(_.filterNot(_ < 32).replace(HydroSink.Delimiter, ""))))
          case v => v
        }
      )
    )
  }

  val eavtReader = delimitedThermometerRecordReader[Eavt]('|', "\\N", implicitly[Decode[Eavt]])
  def valuePipe(vs: NonEmptyList[FeatureValue[Value]], dateTime: DateTime) =
    TypedPipe.from(vs.list.map(v => v -> dateTime.getMillis))

  def featureValuesOnDiskMatch =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], hydroConfig: HydroSink.Config, dateTime: DateTime) => {
      val expected = vs.map(HydroSink.toEavt(_, dateTime.getMillis)).list

      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = HydroSink(hydroConfig)
        executesSuccessfully(sink.write(valuePipe(vs, dateTime)))
        facts(
          path(s"${hydroConfig.hiveConfig.path}/*/*/*/*") ==> records(eavtReader, expected)
        )
      }
    }}.set(minTestsOk = 5)

  def multipleValueSetsOnDiskMatch =
    forAll { (vs1: NonEmptyList[FeatureValue[Value]],
              vs2: NonEmptyList[FeatureValue[Value]],
              hydroConfig: HydroSink.Config,
              dateTime: DateTime) => {
      val expected = (vs1.list ++ vs2.list).map(HydroSink.toEavt(_, dateTime.getMillis))

      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = HydroSink(hydroConfig)
        executesSuccessfully(
          sink.write(valuePipe(vs1, dateTime)).zip(sink.write(valuePipe(vs2, dateTime)))
        )
        facts(
          path(s"${hydroConfig.hiveConfig.path}/*/*/*/*") ==> records(eavtReader, expected)
        )
      }
    }}.set(minTestsOk = 5)

  def featureValuesInHiveMatch =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], hydroConfig: HydroSink.Config, dateTime: DateTime) => {
      def hiveNull(s: String) = if (s == HydroSink.NullValue) "NULL" else s
      val expected = vs.map(value => {
        val eavt = HydroSink.toEavt(value, dateTime.getMillis)
        val (year, month, day) = HydroSink.partition.extract(eavt)
        List(eavt.entity, eavt.attribute, hiveNull(eavt.value), eavt.time, year, month, day).mkString("\t")
      }).list.toSet

      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = HydroSink(hydroConfig)
        val hiveConf = hydroConfig.hiveConfig
        val query = s"""SELECT * FROM `${hiveConf.database}.${hiveConf.tablename}`"""

        executesSuccessfully(sink.write(valuePipe(vs, dateTime)))
        val actual = executesSuccessfully(Execution.fromHive(Hive.query(query)))
        actual.toSet must_== expected.toSet
      }
    }}.set(minTestsOk = 5)

  def expectedPartitionsMarkedSuccess =
    forAll { (vs: NonEmptyList[FeatureValue[Value]], hydroConfig: HydroSink.Config, dateTime: DateTime) =>  {
      val expectedPartitions = vs.map(v =>
        HydroSink.partition.extract(HydroSink.toEavt(v, dateTime.getMillis))
      ).list.toSet.toSeq
      withEnvironment(path(getClass.getResource("/").toString)) {
        val sink = HydroSink(hydroConfig)
        executesSuccessfully(sink.write(valuePipe(vs, dateTime)))
        facts(
          expectedPartitions.map { case (year, month, day) =>
            path(s"${hydroConfig.hiveConfig.path}/year=$year/month=$month/day=$day/_SUCCESS") ==> exists
          }: _*
        )
      }
    }}.set(minTestsOk = 5)
}
