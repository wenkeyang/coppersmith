package commbank.coppersmith.api

import com.twitter.scalding._
import commbank.coppersmith.Lift


package object scalding {

  type FeatureJobConfig[S] = commbank.coppersmith.scalding.FeatureJobConfig[S]
  type SimpleFeatureJob = commbank.coppersmith.scalding.SimpleFeatureJob

  val ScaldingDataSource = commbank.coppersmith.scalding.ScaldingDataSource
  val HiveTextSource = commbank.coppersmith.scalding.HiveTextSource
  val TypedPipeSource = commbank.coppersmith.scalding.TypedPipeSource
  val HydroSink = commbank.coppersmith.scalding.HydroSink

  implicit val framework: Lift[TypedPipe] = commbank.coppersmith.scalding.lift.scalding
}
