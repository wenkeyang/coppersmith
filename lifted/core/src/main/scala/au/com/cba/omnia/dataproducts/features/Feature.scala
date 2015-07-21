package au.com.cba.omnia.dataproducts.features

import scala.reflect.runtime.universe.{TypeTag, typeOf}

object Feature {
  type Namespace = String
  type Name      = String
  type EntityId  = String
  type Time      = Long

  sealed trait Type
  object Type {
    case object Categorical extends Type
    case object Continuous  extends Type
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
  }
}

import Feature._

object FeatureMetadata {
  sealed trait ValueType
  object ValueType {
    case object IntegralType extends ValueType
    case object DecimalType  extends ValueType
    case object StringType   extends ValueType
  }
}

import FeatureMetadata.ValueType

case class FeatureMetadata[+V <: Value : TypeTag](namespace: Namespace, name: Name, featureType: Type) {
  def valueType =
    typeOf[V] match {
      // Would be nice to get exhaustiveness checking here
      case t if t =:= typeOf[Value.Integral] => ValueType.IntegralType
      case t if t =:= typeOf[Value.Decimal] =>  ValueType.DecimalType
      case t if t =:= typeOf[Value.Str] =>      ValueType.StringType
    }
}

abstract class Feature[S, +V <: Value](val metadata: FeatureMetadata[V]) {
  def generate(source:S): Option[FeatureValue[S, V]]
}

case class FeatureValue[S, +V <: Value](
  feature: Feature[S, V],
  entity:  EntityId,
  value:   V,
  time:    Time
)
