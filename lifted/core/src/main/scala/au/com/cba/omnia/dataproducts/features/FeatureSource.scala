package au.com.cba.omnia.dataproducts.features

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean
import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.scalding.typed.TypedPipe

import au.com.cba.omnia.maestro.api.Partition

import lift.scalding.liftJoin

import Feature.{Conforms, EntityId, Name, Namespace, Time, Type, Value}
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
  def load(conf: FeatureJobConfig[S]): TypedPipe[S]
}

object From {
  def apply[S](): From[S] = From[S](None)
}

case class From[S](filter: Option[S => Boolean] = None) {
  // Common filter
  def where(condition: S => Boolean) =
    copy(filter = filter.map(f => (s: S) => f(s) && condition(s)).orElse(condition.some))

  def featureSetBuilder(namespace: Namespace, entity: S => EntityId, time: S => Time) =
    FeatureSetBuilder(namespace, entity, time)

  def bind(cfg: SourceConfiguration[S]): FeatureSource[S] = FromSource[S](cfg, filter)
}

case class FromSource[S](srcCfg: SourceConfiguration[S],
                         filter: Option[S => Boolean]) extends FeatureSource[S]{
  // TODO: Not specific to From sources - lift up
  def filter(p: S => Boolean): FeatureSource[S] =
    copy(filter = filter.map(f => (s: S) => f(s) && p(s)).orElse(p.some))

  def load(conf: FeatureJobConfig[S]): TypedPipe[S] = {
    val pipe = srcCfg.load(conf)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}

case class FeatureSetBuilder[S](namespace: Namespace, entity: S => EntityId, time: S => Time) {
  def select[FV <% V, V <: Value : TypeTag](value : S => FV) = FeatureBuilder[S, FV, V](this, value)
}

case class FeatureBuilder[S, FV <% V, V <: Value : TypeTag](
  fsBuilder: FeatureSetBuilder[S],
  value: S => FV,
  filter: Option[S => Boolean] = None
) {
  def andWhere(condition: S => Boolean) = where(condition)
  def where(condition: S => Boolean) =
    copy(filter = filter.map(f => (s: S) => f(s) && condition(s)).orElse(condition.some))

  def asFeature[T <: Type](name: Name, featureType: T)(implicit ev: Conforms[T, V]) =
    Patterns.general[S, V, FV](fsBuilder.namespace,
                               name,
                               featureType,
                               fsBuilder.entity,
                               (s: S) => filter.map(_(s)).getOrElse(true).option(value(s): V),
                               fsBuilder.time)
}

case class JoinedFeatureSource[L, R, J : Ordering](
  j: Joined[L, R, J],
  srcCfg: (SourceConfiguration[L], SourceConfiguration[R]),
  filter: ((L, R)) => Boolean =  (in: (L, R)) => true
) extends FeatureSource[(L, R)] {
  // TODO: Not specific to Joined sources - lift up
  def filter(p: ((L, R)) => Boolean): FeatureSource[(L, R)] = copy(filter = s => filter(s) && p(s))

  def load(conf: FeatureJobConfig[(L, R)]): TypedPipe[(L, R)] = {
    val (leftSrc, rightSrc) = srcCfg
    liftJoin(j)(leftSrc.load(conf), rightSrc.load(conf)).filter(filter)
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
