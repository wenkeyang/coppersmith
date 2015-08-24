package au.com.cba.omnia.dataproducts.features

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean
import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.algebird.Aggregator

import Feature.{Conforms, EntityId, Name, Namespace, Time, Type, Value}

object FeatureSetBuilder {
  // The parameter types here (basically any type constructor) might be too broad - consider narrowing
  implicit class FeatureSetBuilderSource[T[_], S](t: T[S]) {
    def featureSetBuilder(namespace: Namespace, entity: S => EntityId, time: S => Time) =
      FeatureSetBuilder(namespace, entity, time)
  }
}

case class FeatureSetBuilder[S](namespace: Namespace, entity: S => EntityId, time: S => Time) {
  def apply[FV <% V, V <: Value : TypeTag](value : S => FV): FeatureBuilder[S, FV, V] =
    FeatureBuilder(this, value)

  def apply[T, U <% V, V <: Value : TypeTag](
      aggregator: Aggregator[S, T, U]): AggregationFeatureBuilder[S, T, U, V] =
    AggregationFeatureBuilder(this, aggregator)
}

case class FeatureBuilder[S, FV <% V, V <: Value : TypeTag](
  fsBuilder: FeatureSetBuilder[S],
  value:     S => FV,
  filter:    Option[S => Boolean] = None
) {
  def andWhere(condition: S => Boolean) = where(condition)
  def where(condition: S => Boolean) =
    copy(filter = filter.map(f => (s: S) => f(s) && condition(s)).orElse(condition.some))

  def asFeature[T <: Type](featureType: T, name: Name, description: String)(implicit ev: Conforms[T, V]) =
    Patterns.general[S, V, FV](fsBuilder.namespace,
                               name,
                               description,
                               featureType,
                               fsBuilder.entity,
                               (s: S) => filter.map(_(s)).getOrElse(true).option(value(s): V),
                               fsBuilder.time)
}

case class AggregationFeatureBuilder[S, T, U <% V, V <: Value : TypeTag](
  fsBuilder:  FeatureSetBuilder[S],
  aggregator: Aggregator[S, T, U],
  filter:     Option[S => Boolean] = None
) {
  def andWhere(condition: S => Boolean) = where(condition)
  def where(condition: S => Boolean) =
    copy(filter = filter.map(f => (s: S) => f(s) && condition(s)).orElse(condition.some))

  def asFeature[FT <: Type](featureType: FT, name: Name, description: String)(implicit ev: Conforms[FT, V]) =
    AggregationFeature(name, description, aggregator.andThenPresent(u => u: V), featureType, filter)
}
