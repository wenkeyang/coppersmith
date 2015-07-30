package au.com.cba.omnia.dataproducts.features

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean

import au.com.cba.omnia.maestro.api.Field

import Feature._

trait FeatureSet[S] {
  def namespace: Feature.Namespace

  def features: Iterable[Feature[S, Value]]

  def generate(source: S): Iterable[FeatureValue[S, Value]] = features.flatMap(f =>
    f.generate(source)
  )

  def generateMetadata: Iterable[FeatureMetadata[Value]] = {
    features.map(_.metadata)
  }
}

trait PivotFeatureSet[S] extends FeatureSet[S] {
  def entity(s: S): EntityId
  def time(s: S):   Time

  def pivot[V <: Value : TypeTag, FV <% V](field: Field[S, FV], fType: Feature.Type) =
    Patterns.pivot(namespace, fType, entity, time, field)
}

abstract class QueryFeatureSet[S, V <: Value : TypeTag] extends FeatureSet[S] {
  type Filter = S => Boolean

  def featureType:  Feature.Type

  def entity(s: S): EntityId
  def value(s: S):  V
  def time(s: S):   Time

  def queryFeature(featureName: Feature.Name, filter: Filter) =
    Patterns.general(namespace, featureName, featureType, entity, (s: S) => filter(s).option(value(s)), time)
}

import scalaz.syntax.foldable1.ToFoldable1Ops
import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.algebird.{Aggregator, AveragedValue, Monoid, Semigroup}

case class AggregationFeature[S, U, +V <: Value : TypeTag](
  name:        Name,
  aggregator:  Aggregator[S, U, V],
  featureType: Type = Type.Continuous,
  where:       Option[S => Boolean] = None
) {
  import AggregationFeature.AlgebirdSemigroup
  // Note: Implementation here to satisfty feature signature. Framework should take advantage of
  // the fact that aggregators should be able to be run natively on the underlying plumbing
  def toFeature(namespace: Namespace, time: S => Time) =
      new Feature[(EntityId, Iterable[S]), Value](FeatureMetadata(namespace, name, featureType)) {
    def generate(s: (EntityId, Iterable[S])): Option[FeatureValue[(EntityId, Iterable[S]), Value]] = {
      val source = s._2.filter(where.getOrElse(_ => true)).toList.toNel
      source.map(nonEmptySource => {
        val value = aggregator.present(
          nonEmptySource.foldMap1(aggregator.prepare)(aggregator.semigroup.toScalaz)
        )
        FeatureValue(this, s._1, value, time(nonEmptySource.head))
      })
    }
  }
}

trait AggregationFeatureSet[S] extends FeatureSet[(EntityId, Iterable[S])] {
  def entity(s: S): EntityId
  def time(s: S):   Time

  def aggregationFeatures: Iterable[AggregationFeature[S, _, Value]]

  def features = aggregationFeatures.map(_.toFeature(namespace, time))

  // These allow aggregators to be created without specifying type args that
  // would otherwise be required if calling the delegated methods directly
  def size: Aggregator[S, Long, Long] = Aggregator.size
  def count(where: S => Boolean = _ => true): Aggregator[S, Long, Long] = Aggregator.count(where)
  def avg[V](v: S => Double): Aggregator[S, AveragedValue, Double] = AggregationFeature.avg[S](v)
  def max[V : Ordering](v: S => V): Aggregator[S, V, V] = AggregationFeature.max[S, V](v)
  def min[V : Ordering](v: S => V): Aggregator[S, V, V] = AggregationFeature.min[S, V](v)
  def sum[V : Monoid]  (v: S => V): Aggregator[S, V, V] = Aggregator.prepareMonoid(v)

}

object AggregationFeature {
  def avg[T](t: T => Double): Aggregator[T, AveragedValue, Double] =
    AveragedValue.aggregator.composePrepare[T](t)

  def max[T, V : Ordering](v: T => V): Aggregator[T, V, V] = Aggregator.max[V].composePrepare[T](v)
  def min[T, V : Ordering](v: T => V): Aggregator[T, V, V] = Aggregator.min[V].composePrepare[T](v)


  // TODO: Would be surprised if this doesn't exist elsewhere
  implicit class AlgebirdSemigroup[T](s: Semigroup[T]) {
    def toScalaz = new scalaz.Semigroup[T] { def append(t1: T, t2: =>T): T = s.plus(t1, t2) }
  }

  implicit class FeatureBuilder[S, T, U <% V, V <: Value : TypeTag](aggregator: Aggregator[S, T, U]) {
    def asFeature(featureName: Feature.Name) =
      AggregationFeature(featureName, aggregator.andThenPresent(u => u: V))
  }

  implicit class WhereExtender[S, U, V <: Value : TypeTag](af: AggregationFeature[S, U, V]) {
    def andWhere(where: S => Boolean) = AggregationFeature(
      af.name,
      af.aggregator,
      af.featureType,
      af.where.map(existing => (s: S) => existing(s) && where(s)).orElse(where.some))
  }
}
