package au.com.cba.omnia.dataproducts.features

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, Execution, MultipleTextLineFiles, TupleSetter, TupleConverter}
import com.twitter.scalding.typed._

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.core.partition.HivePartition

import au.com.cba.omnia.etl.util.{ParseUtils, SimpleMaestroJob}

import lift.scalding._

import Join.Joined

object FeatureSource {
  implicit class RichJoined[L, R, J: Ordering](j: Joined[L, R, J]) {
    def bind(cfg: (SourceConfiguration[L], SourceConfiguration[R]),
             filter: ((L, R)) => Boolean = (in: (L, R)) => true): FeatureSource[(L, R)] =
      JoinedFeatureSource(j, cfg, filter)
  }

  case class PartitionPath[S, P](underlying: Partition[S, P], value: P)(implicit ev: PartitionToPath[P]) {
    def toPath = new Path(underlying.pattern.format(ev.toPathComponents((value)): _*))
  }

  case class PartitionToPath[P](toPathComponents: P => List[String])
  import shapeless.syntax.std.tuple.productTupleOps
  implicit val StringToPath       = PartitionToPath[String](List(_))
  implicit val StringTuple2ToPath = PartitionToPath[(String, String)](_.toList)
  implicit val StringTuple3ToPath = PartitionToPath[(String, String, String)](_.toList)
  implicit val StringTuple4ToPath = PartitionToPath[(String, String, String, String)](_.toList)
}

trait FeatureSource[S] {
  def filter(p: S => Boolean): FeatureSource[S]
  def load(conf: FeatureJobConfig[S]): Execution[TypedPipe[S]]
}

case class JoinedFeatureSource[L, R, J : Ordering](
  j: Joined[L, R, J],
  srcCfg: (SourceConfiguration[L], SourceConfiguration[R]),
  filter: ((L, R)) => Boolean =  (in: (L, R)) => true
) extends FeatureSource[(L, R)] {
  // TODO: Not specific to Joined sources - lift up
  def filter(p: ((L, R)) => Boolean): FeatureSource[(L, R)] = copy(filter = s => filter(s) && p(s))

  def load(conf: FeatureJobConfig[(L, R)]): Execution[TypedPipe[(L, R)]] = {
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
  def load(conf: FeatureJobConfig[S]): Execution[TypedPipe[(Feature.EntityId, Iterable[S])]] = {
    underlying.load(conf).map(_.groupBy(grouping))
  }
}
*/

trait SourceConfiguration[S] {
  def load(conf: FeatureJobConfig[_]): Execution[TypedPipe[S]]
}

case class HiveTextSource[S <: ThriftStruct : Decode, P](
  basePath:  Path,
  partition: FeatureSource.PartitionPath[S, P],
  delimiter: String = "|",
  filter:    S => Boolean = (_: S) => true
) extends SourceConfiguration[S] {
  def filter(f: S => Boolean): HiveTextSource[S, P] = copy(filter = (s: S) => filter(s) && f(s))
  def load(conf: FeatureJobConfig[_]) = {
    val inputPath = new Path(basePath, partition.toPath)
    // FIXME: This implementation completely ignores errors
    Execution.from {
      ParseUtils.decodeHiveTextTable[S](
        MultipleTextLineFiles(inputPath.toString, delimiter)
      ).rows.filter(filter)
    }
  }
}

case class HiveParquetSource[S <: ThriftStruct : Decode : Tag : Manifest, P](
  basePath:   Path,
  partition:  FeatureSource.PartitionPath[S, P],
  loadConfig: Either[MaestroConfig, LoadConfig[S]],
  filter:     S => Boolean = (_: S) => true
) extends SourceConfiguration[S] {
  def filter(f: S => Boolean): HiveParquetSource[S, P] = copy(filter = (s: S) => filter(s) && f(s))
  def load(conf: FeatureJobConfig[_]) = {
    val config = loadConfig.left.map(maestro =>
      maestro.load[S](errorThreshold = 0.05)
    ).merge
    for {
      (input, loadInfo) <- Maestro.load[S](config, List(new Path(basePath, partition.toPath).toString))
      _                 <- loadInfo.withSuccess
    } yield input
  }
}
