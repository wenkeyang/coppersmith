package commbank.coppersmith

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean
import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.algebird.Aggregator

import Feature.{Conforms, Description, EntityId, Name, Namespace, Type, Value}

abstract class FeatureBuilderSource[S : TypeTag] {
  def featureSetBuilder(namespace: Namespace, entity: S => EntityId) =
    FeatureSetBuilder[S, S](namespace, entity, { case s => s })
}

object FeatureBuilderSource extends FeatureBuilderSourceInstances

trait FeatureBuilderSourceInstances {
  implicit def fromFS[S : TypeTag](fs: FeatureSource[S, _]) = new FeatureBuilderSource[S] {}
}

/**
  * @tparam S Feature set Source
  * @tparam SV View of Source from which to generate feature
  */
case class FeatureSetBuilder[S : TypeTag, SV](
  namespace: Namespace,
  entity:    S => EntityId,
  view:      PartialFunction[S, SV]
) {
  def map[SVV](f: Function[SV, SVV]): FeatureSetBuilder[S, SVV] = copy(view = view.andThen(f))

  def collect[SVV](pf: PartialFunction[SV, SVV]): FeatureSetBuilder[S, SVV] =
    copy(view =
      // PartialFunction composition. Adapted from http://stackoverflow.com/a/23024859/78398
      Function.unlift(view.lift(_).flatMap(pf.lift))
    )

  def apply[FV <% V, V <: Value : TypeTag](value : SV => FV): FeatureBuilder[S, SV, FV, V] =
    FeatureBuilder(this, value, view)

  def apply[T, U <% V, V <: Value : TypeTag](
      aggregator: Aggregator[SV, T, U]): AggregationFeatureBuilder[S, SV, T, U, V] =
    AggregationFeatureBuilder(this, aggregator, view)

  def select = this

  def selectSource[V <: Value : TypeTag](implicit ev: SV => V) = apply(identity(_))

  // These allow aggregators to be created without specifying type args that
  // would otherwise be required if calling the delegated methods directly
  import com.twitter.algebird.Monoid
  import Aggregator.{count => Count, prepareMonoid => PrepareMonoid, size => Size}
  import AggregationFeature.{avg => Avg, max => Max, min => Min, uniqueCountBy => UniqueCountBy}
  def size                                                       = select(Size)
  def count(where: SV => Boolean = _ => true)                    = select(Count(where))
  def uniqueCountBy[T](f : SV => T)                              = select(UniqueCountBy(f))
  def avg[V](v: SV => Double)                                    = select(Avg[SV](v))
  def max[FV <% V : Ordering, V <: Value : TypeTag](v: SV => FV) = select(Max[SV, FV](v))
  def min[FV <% V : Ordering, V <: Value : TypeTag](v: SV => FV) = select(Min[SV, FV](v))
  def sum[FV <% V : Monoid,   V <: Value : TypeTag](v: SV => FV) = select(PrepareMonoid(v))
}

/**
  * @tparam S Feature Source
  * @tparam SV Feature Source View
  * @tparam FV Raw type of feature value
  * @tparam V Type of Feature Value
  */
case class FeatureBuilder[S : TypeTag, SV, FV <% V, V <: Value : TypeTag](
  fsBuilder: FeatureSetBuilder[S, SV],
  value:     SV => FV,
  view:      PartialFunction[S, SV],
  filter:    Option[SV => Boolean] = None
) {
  def andWhere(condition: SV => Boolean) = where(condition)
  def where(condition: SV => Boolean) =
    copy(filter = filter.map(f => (s: SV) => f(s) && condition(s)).orElse(condition.some))

  def asFeature[T <: Type](featureType: T, name: Name, desc: Description)(implicit ev: Conforms[T, V]) =
    Patterns.general[S, V, FV](fsBuilder.namespace,
                               name,
                               desc,
                               featureType,
                               fsBuilder.entity,
                               (s: S) => view.lift(s).filter(sv => filter.forall(_(sv))).map(value(_): V))
}

/**
  * @tparam S  Feature Source
  * @tparam SV Feature Source View
  * @tparam T  Aggegregator accumulator
  * @tparam FV Raw type of feature value
  * @tparam V  Type of Feature Value
  */
case class AggregationFeatureBuilder[S : TypeTag, SV, T, FV <% V, V <: Value : TypeTag](
  fsBuilder:  FeatureSetBuilder[S, SV],
  aggregator: Aggregator[SV, T, FV],
  view:       PartialFunction[S, SV],
  filter:     Option[SV => Boolean] = None
) {
  def andWhere(condition: SV => Boolean) = where(condition)
  def where(condition: SV => Boolean) =
    copy(filter = filter.map(f => (s: SV) => f(s) && condition(s)).orElse(condition.some))

  def asFeature[FT <: Type](featureType: FT, name: Name, desc: Description)(implicit ev: Conforms[FT, V]) =
    AggregationFeature(name, desc, aggregator.andThenPresent(fv => fv: V), view, featureType, filter)
}
