package commbank.coppersmith

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean

import au.com.cba.omnia.maestro.api.Field

import Feature._

trait FeatureSet[S] extends MetadataSet {
  def namespace: Feature.Namespace

  def features: Iterable[Feature[S, Value]]

  def generate(source: S): Iterable[FeatureValue[Value]] = features.flatMap(f =>
    f.generate(source)
  )
  def metadata: Iterable[FeatureMetadata[Value]] = {
    features.map(_.metadata)
  }
}

trait MetadataSet {
  def metadata: Iterable[FeatureMetadata[Value]]
}

trait PivotFeatureSet[S] extends FeatureSet[S] {
  def entity(s: S): EntityId
  def time(s: S):   Time

  def pivot[V <: Value : TypeTag, FV <% V](field: Field[S, FV], humanDescription: String, fType: Feature.Type) =
    Patterns.pivot(namespace, fType, entity, time, field, humanDescription)
}

abstract class QueryFeatureSet[S, V <: Value : TypeTag] extends FeatureSet[S] {
  type Filter = S => Boolean

  def featureType:  Feature.Type

  def entity(s: S): EntityId
  def value(s: S):  V
  def time(s: S):   Time

  def queryFeature(featureName: Feature.Name, humanDescription: String, filter: Filter) =
    Patterns.general(namespace, featureName, humanDescription, featureType, entity, (s: S) => filter(s).option(value(s)), time)
}

import scalaz.syntax.foldable1.ToFoldable1Ops
import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.algebird.{Aggregator, AveragedValue, Monoid, Semigroup}

case class AggregationFeature[S, U, +V <: Value : TypeTag](
  name:        Name,
  humanDescription: String,
  aggregator:  Aggregator[S, U, V],
  featureType: Type = Type.Continuous,
  where:       Option[S => Boolean] = None
) {
  import AggregationFeature.AlgebirdSemigroup
  // Note: Implementation here to satisfty feature signature. Framework should take advantage of
  // the fact that aggregators should be able to be run natively on the underlying plumbing
  def toFeature(namespace: Namespace, time: S => Time) =
      new Feature[(EntityId, Iterable[S]), Value](FeatureMetadata(namespace, name, humanDescription, featureType)) {
    def generate(s: (EntityId, Iterable[S])): Option[FeatureValue[Value]] = {
      val source = s._2.filter(where.getOrElse(_ => true)).toList.toNel
      source.map(nonEmptySource => {
        val value = aggregator.present(
          nonEmptySource.foldMap1(aggregator.prepare)(aggregator.semigroup.toScalaz)
        )
        FeatureValue(s._1, name, value, time(nonEmptySource.head))
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
  def avg[V](v: S => Double): Aggregator[S, AveragedValue, Double]      = AggregationFeature.avg[S](v)
  def max[V : Ordering](v: S => V): Aggregator[S, V, V]                 = AggregationFeature.max[S, V](v)
  def min[V : Ordering](v: S => V): Aggregator[S, V, V]                 = AggregationFeature.min[S, V](v)
  def sum[V : Monoid]  (v: S => V): Aggregator[S, V, V]                 = Aggregator.prepareMonoid(v)
  def uniqueCountBy[T](f : S => T): Aggregator[S, Set[T], Int]          = AggregationFeature.uniqueCountBy(f)
}

object AggregationFeature {
  def avg[T](t: T => Double): Aggregator[T, AveragedValue, Double] =
    AveragedValue.aggregator.composePrepare[T](t)

  def max[T, V : Ordering](v: T => V): Aggregator[T, V, V]              = Aggregator.max[V].composePrepare[T](v)
  def min[T, V : Ordering](v: T => V): Aggregator[T, V, V]              = Aggregator.min[V].composePrepare[T](v)
  def uniqueCountBy[S, T](f : S => T): Aggregator[S, Set[T], Int]       = Aggregator.uniqueCount[T].composePrepare(f)

  // TODO: Would be surprised if this doesn't exist elsewhere
  implicit class AlgebirdSemigroup[T](s: Semigroup[T]) {
    def toScalaz = new scalaz.Semigroup[T] { def append(t1: T, t2: =>T): T = s.plus(t1, t2) }
  }

}
