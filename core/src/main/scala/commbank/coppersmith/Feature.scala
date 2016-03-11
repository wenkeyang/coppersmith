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

import scala.annotation.implicitNotFound
import scala.collection.immutable.ListSet
import scala.reflect.runtime.universe.{TypeTag, Type => ScalaType, typeOf}

import scalaz.{Name => _, Value => _, _}, Scalaz._, Order.orderBy

import shapeless.=:!=

object Feature {
  type Namespace   = String
  type Name        = String
  type Description = String
  type EntityId    = String
  type Time        = Long


  sealed trait Type
  object Type {
    sealed trait Categorical extends Type
    sealed trait Numeric     extends Type

    case object Continuous extends Numeric
    case object Discrete   extends Numeric

    case object Ordinal extends Categorical
    case object Nominal extends Categorical
  }

  sealed trait Value
  object Value {
    case class Integral(value: Option[Long])  extends Value
    case class Decimal(value: Option[Double]) extends Value
    case class Str(value: Option[String])     extends Value

    implicit def fromInt(i: Int):                Integral = Option(i)
    implicit def fromLong(l: Long):              Integral = Option(l)
    implicit def fromDouble(d: Double):          Decimal  = Option(d)
    implicit def fromString(s: String):          Str      = Option(s)
    implicit def fromOInt(i: Option[Int]):       Integral = Integral(i.map(_.toLong))
    implicit def fromOLong(l: Option[Long]):     Integral = Integral(l)
    implicit def fromODouble(d: Option[Double]): Decimal  = Decimal(d)
    implicit def fromOString(s: Option[String]): Str      = Str(s)

    implicit val intOrder: Order[Integral] = orderBy(_.value)
    implicit val decOrder: Order[Decimal] = orderBy(_.value)
    implicit val strOrder: Order[Str] = orderBy(_.value)

    abstract class Range[+V : Order] {
      // V needs to be covariant to satisfy Metadata type constraint, so can't be in contravariant
      // position here. This problem goes away when switching to arbitrary value types.
      // def contains(v: V): Boolean
      def widestValueSize: Option[Int]
    }
    case class MinMaxRange[V : Order](min: V, max: V) extends Range[V] {
      def contains(v: V) = v >= min && v <= max
      def widestValueSize = None
    }
    case class SetRange[V : Order](values: ListSet[V]) extends Range[V] {
      def contains(v: V) = values.contains(v)
      def widestValueSize = values.collect {
        case Str(s) => s.map(_.length).getOrElse(0)
      }.toList.toNel.map(_.foldRight1(math.max(_, _)))
    }
    object SetRange {
      // Should return Range[V] once V is made invariant on Range and contains is added back
      def apply[V : Order](values: List[V]): SetRange[V] = SetRange(ListSet(values: _*))
    }
  }

  // Legal type/value combinations
  @implicitNotFound("Features with value type ${V} cannot be ${T}")
  abstract class Conforms[T <: Type : TypeTag, V <: Value : TypeTag] {
    def typeTag:  TypeTag[T] = implicitly
    def valueTag: TypeTag[V] = implicitly
  }
  implicit object NominalStr         extends Conforms[Type.Nominal.type,    Value.Str]

  implicit object OrdinalDecimal     extends Conforms[Type.Ordinal.type,    Value.Decimal]
  implicit object ContinuousDecimal  extends Conforms[Type.Continuous.type, Value.Decimal]

  implicit object OrdinalIntegral    extends Conforms[Type.Ordinal.type,    Value.Integral]
  implicit object ContinuousIntegral extends Conforms[Type.Continuous.type, Value.Integral]

  implicit object DiscreteIntegral   extends Conforms[Type.Discrete.type,   Value.Integral]

  object Conforms {

    def conforms_?[V <: Value : TypeTag](conforms: Conforms[_, _], metadata: Metadata[_, _]) = {
      def getClazz(tag: TypeTag[_]) = tag.mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
      metadata.featureType.getClass == getClazz(conforms.typeTag) &&
        metadata.valueType == Metadata.valueType[V]
    }
  }

  implicit class RichFeature[S : TypeTag, V <: Value : TypeTag](f: Feature[S, V]) {
    def as[T <: Feature.Type](t: T)(implicit ev: Conforms[T, V], neq: T =:!= Nothing) = {
      val oldMetadata = f.metadata
      val newMetadata = Metadata[S, V](
        namespace   = oldMetadata.namespace,
        name        = oldMetadata.name,
        description = oldMetadata.description,
        featureType = t
      )
      new Feature[S, V](newMetadata) {
        def generate(source: S) = f.generate(source)
      }
    }
  }

  object Metadata {
    def valueType[V <: Value : TypeTag]: ValueType = typeOf[V] match {
        // Would be nice to get exhaustiveness checking here
        case t if t =:= typeOf[Value.Integral] => ValueType.IntegralType
        case t if t =:= typeOf[Value.Decimal] =>  ValueType.DecimalType
        case t if t =:= typeOf[Value.Str] =>      ValueType.StringType
      }

    sealed trait ValueType
    object ValueType {
      case object IntegralType extends ValueType
      case object DecimalType  extends ValueType
      case object StringType   extends ValueType
    }

    case class TypeInfo(typeName: String, typeArgs: List[TypeInfo]) {
      override def toString = typeName + typeArgs.toNel.map(_.list.mkString("[", ",", "]")).getOrElse("")
    }
    object TypeInfo {
      def apply[T : TypeTag]: TypeInfo = TypeInfo(implicitly[TypeTag[T]].tpe)
      def apply(t: ScalaType): TypeInfo = TypeInfo(t.typeSymbol.fullName, t.typeArgs.map(TypeInfo(_)))
    }

    def apply[S : TypeTag, V <: Value : TypeTag](
      namespace:   Namespace,
      name:        Name,
      description: Description,
      featureType: Type,
      valueRange:  Option[Value.Range[V]] = None
    )(implicit neq: V =:!= Nothing): Metadata[S, V] = {
      Metadata[S, V](namespace, name, description, featureType, valueType[V], valueRange, TypeInfo.apply[S])
    }
  }

  import Metadata.{TypeInfo, ValueType}

  // Hold references to basic source and value type instances instead of requiring
  // TagType instances, as the latter can cause serialisation regressions in some
  // cases where the metadata is closed over.
  case class Metadata[S, +V <: Value] private(
    namespace:   Namespace,
    name:        Name,
    description: Description,
    featureType: Feature.Type,
    valueType:   ValueType,
    valueRange:  Option[Value.Range[V]],
    sourceType:  TypeInfo
  )
}

import Feature._

abstract class Feature[S, +V <: Value](val metadata: Metadata[S, V]) {
  def generate(source:S): Option[FeatureValue[V]]
}

case class FeatureValue[+V <: Value](
  entity:  EntityId,
  name:    Name,
  value:   V
)

object FeatureValue {
  implicit class AsEavt[V <: Value](fv: FeatureValue[V]) {
    def asEavt(time: Time): (EntityId, Name, V, Time) = (fv.entity, fv.name, fv.value, time)
  }
}
