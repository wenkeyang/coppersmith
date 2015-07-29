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

import com.twitter.algebird.{Aggregator, Semigroup}

case class AggregationFeature[S, V <: Value](
  metadata:   FeatureMetadata[V],
  aggregator: Aggregator[S, V, V],
  andWhere:   Option[S => Boolean] = None
)

trait AggregatorQueryFeatureSet[S] extends FeatureSet[(EntityId, Iterable[S])] {
  type Filter = S => Boolean

  def featureType:  Feature.Type

  def groupBy(s: S): EntityId
  def where(s: S):   Boolean
  def time(s: S):    Time

  def feature[V <: Value : TypeTag](name:       Feature.Name,
                                    aggregator: Aggregator[S, V, V],
                                    andWhere:   Option[Filter] = None) =
    AggregationFeature(FeatureMetadata(namespace, name, featureType), aggregator, andWhere)

  implicit class FeatureBuilder[V <: Value : TypeTag](aggregator: Aggregator[S, V, V]) {
    def withName(featureName: Feature.Name) = feature(featureName, aggregator)
  }

  implicit class WhereExtender[V <: Value](af: AggregationFeature[S, V]) {
    def andWhere(where: Filter) = AggregationFeature(
      af.metadata,
      af.aggregator,
      af.andWhere.map(existing => (s: S) => existing(s) && where(s)).orElse(where.some))
  }

  // TODO: Would be surprised if this doesn't exist elsewhere
  implicit class AlgebirdSemigroup[T](s: Semigroup[T]) {
    def toScalaz = new scalaz.Semigroup[T] { def append(t1: T, t2: =>T): T = s.plus(t1, t2) }
  }

  def aggregationFeatures: Iterable[AggregationFeature[S, Value]]

  // Note: Implementation here to satisfty feature signature. Framework should take advantage of
  // the fact that aggregators should be able to be run natively on the underlying plumbing
  def features = aggregationFeatures.map(af =>
    new Feature[(EntityId, Iterable[S]), Value](af.metadata) {
      def generate(s: (EntityId, Iterable[S])): Option[FeatureValue[(EntityId, Iterable[S]), Value]] = {
        // FIXME: first where filter should be applied around the whole set
        val source = s._2.filter(where).filter(af.andWhere.getOrElse(_ => true)).toList.toNel
        source.map(nonEmptySource => {
          val value = nonEmptySource.foldMap1(af.aggregator.prepare)(af.aggregator.semigroup.toScalaz)
          FeatureValue(this, groupBy(nonEmptySource.head), value, time(nonEmptySource.head))
        })
      }
    }
  )
}
