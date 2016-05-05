package commbank.coppersmith.scalding

import commbank.coppersmith.scalding.HiveSupport.{FailJob, DelimiterConflictStrategy}
import org.apache.hadoop.fs.Path
import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api._, Maestro._

import commbank.coppersmith.{FeatureValue, Feature}, Feature._, Value._
import commbank.coppersmith.thrift.Eavt

/**
  * Created by vaughaew on 4/05/2016.
  */

object EavtTextSink {
  import TextSink._

  val defaultPartition = DerivedSinkPartition[Eavt, (String, String, String)](
    HivePartition.byDay(Fields[Eavt].Time, "yyyy-MM-dd")
  )

  type EavtTextSink = TextSink[Eavt]

  implicit object EavtEnc extends FeatureValueEnc[Eavt] {
    def encode(fvt: (FeatureValue[_], Time)): Eavt = fvt match {
      case (fv, time) =>
        val featureValue = (fv.value match {
          case Integral(v) => v.map(_.toString)
          case Decimal(v) => v.map(_.toString)
          case Str(v) => v
        }).getOrElse(TextSink.NullValue)

        // TODO: Does time format need to be configurable?
        val featureTime = new DateTime(time).toString("yyyy-MM-dd")
        Eavt(fv.entity, fv.name, featureValue, featureTime)
    }
  }

  def configure(dbPrefix:  String,
                 dbRoot:    Path,
                 tableName: TableName,
                 partition: SinkPartition[Eavt] = defaultPartition,
                 group:     Option[String] = None,
                 dcs:       DelimiterConflictStrategy[Eavt] = FailJob[Eavt]()): EavtTextSink = {
    TextSink.configure(dbPrefix, dbRoot, tableName, partition, group, dcs)
  }

}