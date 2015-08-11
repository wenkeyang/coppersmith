package au.com.cba.omnia.dataproducts.features

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, Execution}
import com.twitter.scalding.typed._

import au.com.cba.omnia.maestro.api._

import au.com.cba.omnia.etl.util.SimpleMaestroJob

trait FeatureJobConfig {
  def sourcePath:  Path
  def featureSink: FeatureSink
}

abstract class SimpleFeatureJob extends SimpleMaestroJob {
  def generate[S1, S2, J](
    cfg:      Config => FeatureJobConfig,
    source:   FeatureSource[(S1, S2)],
    features: AggregationFeatureSet[(S1, S2)]
  ) = for {
      conf     <- Execution.getConfig.map(cfg)
      input    <- source.load(conf)
      features <- Execution.from { sys.error(""): TypedPipe[FeatureValue[_, _]] }
      _        <- conf.featureSink.write(features, conf)
    } yield JobFinished
}
