package commbank.coppersmith.examples.userguide

import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{Execution, TypedTsv}
import commbank.coppersmith.Feature.Time
import commbank.coppersmith.Feature.Value.{Decimal, Integral, Str}
import commbank.coppersmith.FeatureValue
import commbank.coppersmith.scalding.FeatureSink

case class FlatFeatureSink(output: String) extends FeatureSink {
  override def write(features: TypedPipe[(FeatureValue[_], Time)]): Execution[Unit] = {

    val featurePipe = features.map { case (fv, t) =>
      val featureValue = (fv.value match {
        case Integral(v) => v.map(_.toString)
        case Decimal(v)  => v.map(_.toString)
        case Str(v)      => v
      }).getOrElse("")
      s"${fv.entity}|${fv.name}|${featureValue}"
    }
    featurePipe.writeExecution(TypedTsv[String](output)).unit
  }
}
