package commbank.coppersmith.scalding

import com.twitter.scalding._
import commbank.coppersmith.{scalding, Lift}


package object api {

  type FeatureJobConfig[S] = commbank.coppersmith.scalding.FeatureJobConfig[S]
  type SimpleFeatureJob = commbank.coppersmith.scalding.SimpleFeatureJob

  val ScaldingDataSource = commbank.coppersmith.scalding.ScaldingDataSource
  val HiveTextSource = commbank.coppersmith.scalding.HiveTextSource
  val HydroSink = commbank.coppersmith.scalding.HydroSink

  implicit val framework: Lift[TypedPipe] = scalding.lift.scalding

}
