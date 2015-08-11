package au.com.cba.omnia.dataproducts.features

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, Execution, MultipleTextLineFiles, TupleSetter, TupleConverter}
import com.twitter.scalding.typed._

import au.com.cba.omnia.maestro.api._

import au.com.cba.omnia.etl.util.{ParseUtils, SimpleMaestroJob}

import lift.scalding._

import Join.Joined

object FeatureSource {
  implicit class RichJoined[L, R, J: Ordering](j: Joined[L, R, J]) {
    def bind(cfg: (SourceConfiguration[L], SourceConfiguration[R]),
             filter: ((L, R)) => Boolean = (in: (L, R)) => true): FeatureSource[(L, R)] =
      JoinedFeatureSource(j, cfg, filter)
  }
}

trait FeatureSource[S] {
  def filter(p: S => Boolean): FeatureSource[S]
  def load(conf: FeatureJobConfig): Execution[TypedPipe[S]]
}

case class JoinedFeatureSource[L, R, J : Ordering](
  j: Joined[L, R, J],
  srcCfg: (SourceConfiguration[L], SourceConfiguration[R]),
  filter: ((L, R)) => Boolean =  (in: (L, R)) => true
) extends FeatureSource[(L, R)] {
  // TODO: Not specific to Joined sources - lift up
  def filter(p: ((L, R)) => Boolean): FeatureSource[(L, R)] = copy(filter = s => filter(s) && p(s))

  def load(conf: FeatureJobConfig): Execution[TypedPipe[(L, R)]] = {
    val (leftSrc, rightSrc) = srcCfg
    for {
      leftPipe <- leftSrc.load(conf)
      rightPipe <- rightSrc.load(conf)
      joinedPipe = liftJoin(j)(leftPipe, rightPipe)
    } yield joinedPipe.filter(filter)
  }
}

// FIXME: Needs further abstraction of underlying 'TypedPipe' structure, as load would
// return a Grouped instance
/*
case class GroupedFeatureSource[S](
  underlying: FeatureSource[S],
  grouping: S => Feature.EntityId
) extends FeatureSource[(Feature.EntityId, Iterable[S])] {
  def load(conf: FeatureJobConfig): Execution[TypedPipe[(Feature.EntityId, Iterable[S])]] = {
    underlying.load(conf).map(_.groupBy(grouping))
  }
}
*/

trait SourceConfiguration[S] {
  def load(conf: FeatureJobConfig): Execution[TypedPipe[S]]
}

case class HiveTextSource[S <: ThriftStruct : Decode, P](
  tableName: String,
  partition: Partition[S, P],
  filter: S => Boolean = (_: S) => true
) extends SourceConfiguration[S] {
  def filter(f: S => Boolean): HiveTextSource[S, P] = copy(filter = (s: S) => filter(s) && f(s))
  def load(conf: FeatureJobConfig) = Execution.from {
    // FIXME: This implementation completely ignores errors
    ParseUtils.decodeHiveTextTable[S](
      MultipleTextLineFiles(conf.sourcePath.toString + tableName /* + partition...*/)
    ).rows.filter(filter)
  }
}
