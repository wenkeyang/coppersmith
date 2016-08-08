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
import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.algebird.Aggregator

import Feature.{Conforms, Description, EntityId, Name, Namespace, Type, Value}

abstract class FeatureBuilderSource[S] {
  def featureSetBuilder(namespace: Namespace, entity: S => EntityId) =
    FeatureSetBuilder[S, S](namespace, entity, { case s => s })
}

object FeatureBuilderSource extends FeatureBuilderSourceInstances

trait FeatureBuilderSourceInstances {
  implicit def fromFS[S](fs: FeatureSource[S, _]) = new FeatureBuilderSource[S] {}
  implicit def fromCFS[S, C : TypeTag](fs: ContextFeatureSource[S, C, _]) =
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
case class FeatureSetBuilder[S, SV](
    namespace: Namespace,
    entity: S => EntityId,
    view: PartialFunction[S, SV]
) {
  def map[SVV](f: Function[SV, SVV]): FeatureSetBuilder[S, SVV] = copy(view = view.andThen(f))

  def collect[SVV](pf: PartialFunction[SV, SVV]): FeatureSetBuilder[S, SVV] =
    copy(view = view.andThenPartial(pf))

  def apply[FV <% V, V <: Value](value: SV => FV): FeatureBuilder[S, SV, FV, V] =
    FeatureBuilder(this, value, view)

  def apply[T, FV <% V, V <: Value](
      aggregator: Aggregator[SV, T, FV]): AggregationFeatureBuilder[S, SV, T, FV, V] = {
    val agg = new Aggregator[SV, T, Option[FV]] {
      def prepare(s: SV) = aggregator.prepare(s)
      def semigroup      = aggregator.semigroup
      // Lift `T` into `Option` - essentially `.having(_ => true)`
      def present(t: T) = aggregator.present(t).some
    }
    AggregationFeatureBuilder(this, agg, view)
  }

  // For fluent-API, eg, collect{...}.select(...) as opposed to collect{...}(...) or
  // collect{...}.apply(...)
  def select = this

  // Allows feature to be built directly from map or collect without having to specify
  // select(identity(_))
  def asFeature[FT <: Type, V <: Value](
      featureType: FT,
      name: Name,
      range: Option[Value.Range[V]],
      desc: Description
  )(implicit svv: SV => V, ev: Conforms[FT, V], tts: TypeTag[S], ttv: TypeTag[V]): Feature[S, V] =
    apply[SV, V](identity(_)).asFeature(featureType, name, range, desc)

  def asFeature[FT <: Type, V <: Value](
      featureType: FT,
      name: Name,
      desc: Description
  )(implicit svv: SV => V, ev: Conforms[FT, V], tts: TypeTag[S], ttv: TypeTag[V]): Feature[S, V] =
    asFeature(featureType, name, range = None, desc)
}

/**
  * @tparam S  Feature Source
  * @tparam SV Feature Source View
  * @tparam FV Raw type of feature value
  * @tparam V  Type of Feature Value
  */
case class FeatureBuilder[S, SV, FV <% V, V <: Value](
    fsBuilder: FeatureSetBuilder[S, SV],
    value: SV => FV,
    view: PartialFunction[S, SV]
) {
  def andWhere(condition: SV => Boolean) = where(condition)
  def where(condition: SV => Boolean) =
    copy(view = view.andThenPartial { case s if condition(s) => s })

  def asFeature[T <: Type](
      featureType: T,
      name: Name,
      range: Option[Value.Range[V]],
      desc: Description
  )(implicit ev: Conforms[T, V], tts: TypeTag[S], ttv: TypeTag[V]): Feature[S, V] =
    Patterns.general[S, V](fsBuilder.namespace,
                           name,
                           desc,
                           featureType,
                           fsBuilder.entity,
                           (s: S) => view.lift(s).map(value(_): V),
                           range)

  def asFeature[T <: Type](
      featureType: T,
      name: Name,
      desc: Description
  )(implicit ev: Conforms[T, V], tts: TypeTag[S], ttv: TypeTag[V]): Feature[S, V] =
    asFeature(featureType, name, range = None, desc)
}

/**
  * @tparam S  Feature Source
  * @tparam SV Feature Source View
  * @tparam T  Aggegregator accumulator
  * @tparam FV Raw type of feature value
  * @tparam V  Type of Feature Value
  */
case class AggregationFeatureBuilder[S, SV, T, FV <% V, V <: Value](
    fsBuilder: FeatureSetBuilder[S, SV],
    aggregator: Aggregator[SV, T, Option[FV]],
    view: PartialFunction[S, SV]
) {
  def andWhere(condition: SV => Boolean) = where(condition)
  def where(condition: SV => Boolean) =
    copy(view = view.andThenPartial { case s if condition(s) => s })
  def having(condition: T => Boolean) =
    copy(aggregator = new Aggregator[SV, T, Option[FV]] {
      def prepare(s: SV) = aggregator.prepare(s)
      def semigroup      = aggregator.semigroup
      def present(t: T)  = condition(t).option(aggregator.present(t)).flatten
    })

  def asFeature[FT <: Type](
      featureType: FT,
      name: Name,
      range: Option[Value.Range[V]],
      desc: Description
  )(implicit ev: Conforms[FT, V],
    tts: TypeTag[S],
    ttv: TypeTag[V]): AggregationFeature[S, SV, T, V] =
    AggregationFeature(name,
                       desc,
                       aggregator.andThenPresent(fvOpt => fvOpt.map(fv => fv: V)),
                       view,
                       featureType,
                       range)

  def asFeature[FT <: Type](
      featureType: FT,
      name: Name,
      desc: Description
  )(implicit ev: Conforms[FT, V],
    tts: TypeTag[S],
    ttv: TypeTag[V]): AggregationFeature[S, SV, T, V] =
    asFeature(featureType, name, range = None, desc)
}
