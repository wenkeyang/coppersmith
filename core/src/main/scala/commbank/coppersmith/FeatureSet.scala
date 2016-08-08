//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean

import au.com.cba.omnia.maestro.api.Field

import Feature._

trait FeatureSet[S] extends MetadataSet[S] {
  def namespace: Feature.Namespace

  def features: Iterable[Feature[S, Value]]

  def generate(source: S): Iterable[FeatureValue[Value]] =
    features.flatMap(f => f.generate(source))
  def metadata: Iterable[Metadata[S, Value]] = {
    features.map(_.metadata)
  }
}

trait FeatureSetWithTime[S] extends FeatureSet[S] {

  /**
    * Specifies the time associated with a feature. Most of the time that will be
    * the job time, but when it depends on data, this method should be overridden.
    */
  def time(source: S, c: FeatureContext): FeatureTime = c.generationTime.getMillis
}

trait MetadataSet[S] {
  def metadata: Iterable[Metadata[S, Value]]
}

abstract class PivotFeatureSet[S : TypeTag] extends FeatureSetWithTime[S] {
  def entity(s: S): EntityId

  def pivot[V <: Value : TypeTag, FV <% V](field: Field[S, FV],
                                           humanDescription: String,
                                           featureType: Type,
                                           range: Option[Value.Range[V]] = None) =
    Patterns.pivot(namespace, featureType, entity, field, humanDescription, range)
}

abstract class BasicFeatureSet[S : TypeTag] extends FeatureSetWithTime[S] {
  def entity(s: S): EntityId

  def basicFeature[V <: Value : TypeTag](featureName: Name,
                                         humanDescription: String,
                                         featureType: Type,
                                         range: Option[Value.Range[V]])(value: S => V) =
    Patterns.general(namespace,
                     featureName,
                     humanDescription,
                     featureType,
                     entity,
                     (s: S) => Some(value(s)),
                     range)

  def basicFeature[V <: Value : TypeTag](featureName: Name,
                                         humanDescription: String,
                                         featureType: Type)(value: S => V): Feature[S, V] =
    basicFeature(featureName, humanDescription, featureType, None)(value)
}

abstract class QueryFeatureSet[S : TypeTag, V <: Value : TypeTag] extends FeatureSetWithTime[S] {
  type Filter = S => Boolean

  def featureType: Feature.Type

  def entity(s: S): EntityId
  def value(s: S): V

  def queryFeature(featureName: Name, humanDescription: String, range: Option[Value.Range[V]])(
      filter: Filter) =
    Patterns.general(namespace,
                     featureName,
                     humanDescription,
                     featureType,
                     entity,
                     (s: S) => filter(s).option(value(s)),
                     range)

  def queryFeature(featureName: Name, humanDescription: String)(filter: Filter): Feature[S, V] =
    queryFeature(featureName, humanDescription, None)(filter)
}

import scalaz.syntax.foldable1.ToFoldable1Ops
import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.algebird.{Aggregator, AveragedValue, Monoid, Semigroup}

case class AggregationFeature[S : TypeTag, SV, U, +V <: Value : TypeTag](
    name: Name,
    description: Description,
    aggregator: Aggregator[SV, U, Option[V]],
    view: PartialFunction[S, SV],
    featureType: Type,
    range: Option[Value.Range[V]] = None
) {
  import AggregationFeature.AlgebirdSemigroup
  // Note: Implementation exists here to satisfty feature signature and enable unit testing.
  // Framework should take advantage of aggregators that can run natively on the underlying plumbing.
  def toFeature(namespace: Namespace) =
    new Feature[(EntityId, Iterable[S]), Value](
        Metadata(namespace, name, description, featureType, range)
    ) {
      def generate(s: (EntityId, Iterable[S])): Option[FeatureValue[Value]] = {
        val (entity, source) = s
        val sourceView       = source.toList.collect(view).toNel
        sourceView.flatMap(nonEmptySource => {
          val aggregated: U =
            nonEmptySource.foldMap1(aggregator.prepare)(aggregator.semigroup.toScalaz)
          aggregator.present(aggregated).map(FeatureValue(entity, name, _))
        })
      }
    }
}

object BigDecimalMonoid extends Monoid[BigDecimal] {
  def plus(l: BigDecimal, r: BigDecimal) = l + r
  def zero                               = 0L
}

trait AggregationFeatureSet[S] extends FeatureSet[(EntityId, Iterable[S])] {
  import commbank.coppersmith.AggregationFeature.BigDAverages.BigDAveragedValue
  implicit val bigDecimalMonoid = BigDecimalMonoid

  def entity(s: S): EntityId

  def aggregationFeatures: Iterable[AggregationFeature[S, _, _, Value]]

  def features = aggregationFeatures.map(_.toFeature(namespace))

  // These allow aggregators to be created without specifying type args that
  // would otherwise be required if calling the delegated methods directly
  def size: Aggregator[S, Long, Long] =
    Aggregator.size
  def count(where: S => Boolean = _ => true): Aggregator[S, Long, Long] =
    Aggregator.count(where)
  def avg[V](v: S => Double): Aggregator[S, AveragedValue, Double] =
    AggregationFeature.avg[S](v)
  def avgBigDec[V](v: S => BigDecimal): Aggregator[S, BigDAveragedValue, BigDecimal] =
    AggregationFeature.avgBigDec[S](v)
  def max[V : Ordering](v: S => V): Aggregator[S, V, V] =
    AggregationFeature.max[S, V](v)
  def min[V : Ordering](v: S => V): Aggregator[S, V, V] =
    AggregationFeature.min[S, V](v)
  def maxBy[O : Ordering, V](o: S => O)(v: S => V): Aggregator[S, S, V] =
    AggregationFeature.maxBy[S, O, V](o)(v)
  def minBy[O : Ordering, V](o: S => O)(v: S => V): Aggregator[S, S, V] =
    AggregationFeature.minBy[S, O, V](o)(v)
  def sum[V : Monoid](v: S => V): Aggregator[S, V, V] =
    Aggregator.prepareMonoid(v)
  def uniqueCountBy[T](f: S => T): Aggregator[S, Set[T], Int] =
    AggregationFeature.uniqueCountBy(f)
}

object AggregationFeature {
  import BigDAverages._

  def avg[T](t: T => Double): Aggregator[T, AveragedValue, Double] =
    AveragedValue.aggregator.composePrepare[T](t)
  def avgBigDec[T](t: T => BigDecimal): Aggregator[T, BigDAveragedValue, BigDecimal] =
    BigDAverager.composePrepare[T](t)
  def maxBy[T, O : Ordering, V](o: T => O)(v: T => V): Aggregator[T, T, V] =
    Aggregator.maxBy[T, O](o).andThenPresent[V](v)
  def minBy[T, O : Ordering, V](o: T => O)(v: T => V): Aggregator[T, T, V] =
    Aggregator.minBy[T, O](o).andThenPresent[V](v)

  def max[T, V : Ordering](v: T => V): Aggregator[T, V, V] = Aggregator.max[V].composePrepare[T](v)
  def min[T, V : Ordering](v: T => V): Aggregator[T, V, V] = Aggregator.min[V].composePrepare[T](v)
  def uniqueCountBy[S, T](f: S => T): Aggregator[S, Set[T], Int] =
    Aggregator.uniqueCount[T].composePrepare(f)

  // TODO: Would be surprised if this doesn't exist elsewhere
  implicit class AlgebirdSemigroup[T](s: Semigroup[T]) {
    def toScalaz = new scalaz.Semigroup[T] { def append(t1: T, t2: => T): T = s.plus(t1, t2) }
  }

  // Based on com.twitter.algebird.AveragedGroup. Omits the scaling code
  // required for increasing the accuracy of averaging `Double`s, as it is
  // not applicable for the `BigDecimal` implementation.

  object BigDAverages {
    import com.twitter.algebird.{Group, MonoidAggregator}

    object BigDAveragedValue {
      implicit val group = BigDAveragedGroup
      def numericAggregator[N](
          implicit num: Numeric[N]): MonoidAggregator[N, BigDAveragedValue, BigDecimal] =
        Aggregator.prepareMonoid { n: N =>
          new BigDAveragedValue(1L, num.toDouble(n))
        }.andThenPresent(_.value)
    }

    case class BigDAveragedValue(count: Long, value: BigDecimal)

    object BigDAverager extends MonoidAggregator[BigDecimal, BigDAveragedValue, BigDecimal] {
      val monoid = BigDAveragedGroup

      def prepare(value: BigDecimal) = new BigDAveragedValue(1L, value)

      def present(average: BigDAveragedValue) = average.value
    }

    object BigDAveragedGroup extends Group[BigDAveragedValue] {
      val zero = BigDAveragedValue(0L, 0.0)

      override def isNonZero(av: BigDAveragedValue) = (av.count != 0L)

      override def negate(av: BigDAveragedValue) = BigDAveragedValue(-av.count, av.value)

      def plus(cntAve1: BigDAveragedValue, cntAve2: BigDAveragedValue): BigDAveragedValue = {
        val (big, small) =
          if (cntAve1.count >= cntAve2.count)
            (cntAve1, cntAve2)
          else
            (cntAve2, cntAve1)
        val n      = big.count
        val k      = small.count
        val newCnt = n + k
        if (newCnt == n) {
          // Handle zero without allocation
          big
        } else if (newCnt == 0L) {
          zero
        } else {
          val an     = big.value
          val ak     = small.value
          val newAve = (n * an + k * ak) / newCnt
          new BigDAveragedValue(newCnt, newAve)
        }
      }
    }
  }
}
