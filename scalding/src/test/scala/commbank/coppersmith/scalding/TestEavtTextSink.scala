package commbank.coppersmith.scalding

import org.joda.time.DateTime

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.thrift.Eavt

/**
  * Created by vaughaew on 4/05/2016.
  */

object TestEavtTextSink {

  val defaultPartition = DerivedSinkPartition[Eavt, (String, String, String)](
    HivePartition.byDay(Fields[Eavt].Time, "yyyy-MM-dd")
  )

  implicit object EavtEnc extends FeatureValueEnc[Eavt] {
    def encode(fvt: (FeatureValue[_], Time)): Eavt = fvt match {
      case (fv, time) =>
        val featureValue = (fv.value match {
          case Integral(v) => v.map(_.toString)
          case Decimal(v) => v.map(_.toString)
          case Str(v) => v
        }).getOrElse(TextSink.NullValue)

        val featureTime = new DateTime(time).toString("yyyy-MM-dd")
        Eavt(fv.entity, fv.name, featureValue, featureTime)
    }
  }

}