package au.com.cba.omnia.dataproducts.features

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, Execution}
import com.twitter.scalding.typed._

import au.com.cba.omnia.maestro.api._

import au.com.cba.omnia.etl.util.SimpleMaestroJob

import lift.scalding._

import Join.Joined

trait FeatureJobConfig {
  def sourcePath:  Path
  def featureSink: FeatureSink
}

object SimpleFeatureJob {

  case class JoinedFeatureSource[L, R, J : Ordering](
                                           j: Joined[L, R, J],
                                           fmt: (FeatureSource[L], FeatureSource[R]),
                                           filter: ((L, R)) => Boolean =  (in: (L, R)) => true) extends FeatureSource[(L, R)] {
    def filter(p: ((L, R)) => Boolean): FeatureSource[(L, R)] = copy(filter = s => filter(s) && p(s))

    def load(conf: FeatureJobConfig): Execution[TypedPipe[(L, R)]] = {
      val (leftSrc, rightSrc) = fmt
      for {
        leftPipe <- leftSrc.load(conf)
        rightPipe <- rightSrc.load(conf)
        joinedPipe = liftJoin(j)(leftPipe, rightPipe)
      } yield joinedPipe.filter(filter)
    }
  }

  implicit class RichJoined[L, R, J: Ordering](j: Joined[L, R, J]) {
    def asSource(fmt: (FeatureSource[L], FeatureSource[R]),
                 filter: ((L, R)) => Boolean = (in: (L, R)) => true): FeatureSource[(L, R)] =
      JoinedFeatureSource(j, fmt, filter)
  }
}

abstract class SimpleFeatureJob extends SimpleMaestroJob {
  def generate[S1, S2, J](
    cfg:      Config => FeatureJobConfig,
    source:   FeatureSource[(Feature.EntityId, Iterable[(S1, S2)])],
    features: AggregationFeatureSet[(S1, S2)]
  ) = for {
      conf     <- Execution.getConfig.map(cfg)
      input    <- source.load(conf)
      features <- Execution.from { sys.error(""): TypedPipe[FeatureValue[_, _]] }
      _        <- conf.featureSink.write(features, conf)
    } yield JobFinished
}
