package au.com.cba.omnia.dataproducts.features

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, Execution}
import com.twitter.scalding.typed._

import au.com.cba.omnia.maestro.api._

import au.com.cba.omnia.etl.util.SimpleMaestroJob

import Feature.{EntityId, Time}

trait FeatureJobConfig {
  def sourcePath:  Path
  def featureSink: FeatureSink
}

abstract class SimpleFeatureJob extends SimpleMaestroJob {
  def generate[S](
    cfg:      Config => FeatureJobConfig,
    source:   FeatureSource[S],
    features: AggregationFeatureSet[S]
  ): Execution[JobStatus] = for {
      conf     <- Execution.getConfig.map(cfg)
      input    <- source.load(conf)
      grouped   = input.groupBy(s => (features.entity(s), features.time(s)))
      values    = generate(grouped, features)
      _        <- conf.featureSink.write(values, conf)
    } yield JobFinished

  // FIXME: Should be able to take advantage of shapless' tuple support in combination with
  // Aggregator.join in order to run the aggregators in one pass over the input
  private def generate[S](grouped: Grouped[(EntityId, Time), S],
                          features: AggregationFeatureSet[S]): TypedPipe[FeatureValue[_, _]] = {
    features.aggregationFeatures.map(feature => {
      grouped.aggregate(feature.aggregator).toTypedPipe.map { case ((e, t), v) =>
        // FIXME: Change FeatureValue to take name (or metadata) instead of related feature
        // to avoid this unnecessary call to toFeature
        FeatureValue(feature.toFeature(features.namespace, features.time), e, v, t)
      }
    }).foldLeft(TypedPipe.from(List[FeatureValue[_, _]]()))(_.++(_))
  }
}
