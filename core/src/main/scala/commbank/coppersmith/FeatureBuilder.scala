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
  implicit def fromCFS[S : TypeTag, C : TypeTag](fs: ContextFeatureSource[S, C, _]) =
    new FeatureBuilderSource[(S, C)] {}
}

object FeatureSetBuilder {
  // PartialFunction composition (see http://stackoverflow.com/a/23024859/78398)
  implicit class ComposePartial[A, B](pf: PartialFunction[A, B]) {
    def andThenPartial[C](that: PartialFunction[B, C]): PartialFunction[A, C] =
      Function.unlift(pf.lift(_) flatMap that.lift)
  }
}
import FeatureSetBuilder.ComposePartial

/**
  * @tparam S  Feature set Source
  * @tparam SV View of Source from which to generate feature
  */
case class FeatureSetBuilder[S : TypeTag, SV](
  namespace: Namespace,
  entity:    S => EntityId,
  view:      PartialFunction[S, SV]
) {
  def map[SVV](f: Function[SV, SVV]): FeatureSetBuilder[S, SVV] = copy(view = view.andThen(f))

  def collect[SVV](pf: PartialFunction[SV, SVV]): FeatureSetBuilder[S, SVV] =
    copy(view = view.andThenPartial(pf))

  def apply[FV <% V, V <: Value : TypeTag](value : SV => FV): FeatureBuilder[S, SV, FV, V] =
    FeatureBuilder(this, value, view)

  def apply[T, FV <% V, V <: Value : TypeTag](
      aggregator: Aggregator[SV, T, FV]): AggregationFeatureBuilder[S, SV, T, FV, V] =
    AggregationFeatureBuilder(this, aggregator, view)

  // For fluent-API, eg, collect{...}.select(...) as opposed to collect{...}(...) or
  // collect{...}.apply(...)
  def select = this

  // Allows feature to be built directly from map or collect without having to specify
  // select(identity(_))
  def asFeature[FT <: Type, V <: Value : TypeTag](
    featureType: FT,
    name:        Name,
    desc:        Description
  )(implicit svv: SV => V, ev: Conforms[FT, V]) =
    apply[SV, V](identity(_)).asFeature(featureType, name, desc)
}

/**
  * @tparam S  Feature Source
  * @tparam SV Feature Source View
  * @tparam FV Raw type of feature value
  * @tparam V  Type of Feature Value
  */
case class FeatureBuilder[S : TypeTag, SV, FV <% V, V <: Value : TypeTag](
  fsBuilder: FeatureSetBuilder[S, SV],
  value:     SV => FV,
  view:      PartialFunction[S, SV]
) {
  def andWhere(condition: SV => Boolean) = where(condition)
  def where(condition: SV => Boolean) = copy(view = view.andThenPartial { case s if condition(s) => s })

  def asFeature[T <: Type](featureType: T, name: Name, desc: Description)(implicit ev: Conforms[T, V]) =
    Patterns.general[S, V, FV](fsBuilder.namespace,
                               name,
                               desc,
                               featureType,
                               fsBuilder.entity,
                               (s: S) => view.lift(s).map(value(_): V))
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
  view:       PartialFunction[S, SV]
) {
  def andWhere(condition: SV => Boolean) = where(condition)
  def where(condition: SV => Boolean) = copy(view = view.andThenPartial { case s if condition(s) => s })

  def asFeature[FT <: Type](featureType: FT, name: Name, desc: Description)(implicit ev: Conforms[FT, V]) =
    AggregationFeature(name, desc, aggregator.andThenPresent(fv => fv: V), view, featureType)
}
