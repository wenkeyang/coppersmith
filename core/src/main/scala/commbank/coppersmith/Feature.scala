package commbank.coppersmith

import shapeless.=:!=

import scala.reflect.runtime.universe.{TypeTag, typeOf}

object Feature {
  type Namespace   = String
  type Name        = String
  type Description = String
  type EntityId    = String
  type Time        = Long


  def defaultTime[S](s: S, ctx: FeatureContext) = ctx.generationTime.getMillis

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

  // Legal type/value combinations
  sealed trait Conforms[T <: Type, V <: Value]
  implicit object CategoricalStr      extends Conforms[Type.Categorical.type, Value.Str]
  implicit object CategoricalIntegral extends Conforms[Type.Categorical.type, Value.Integral]
  implicit object CategoricalDecimal  extends Conforms[Type.Categorical.type, Value.Decimal]
  implicit object ContinuousIntegral  extends Conforms[Type.Continuous.type,  Value.Integral]
  implicit object ContinuousDecimal   extends Conforms[Type.Continuous.type,  Value.Decimal]

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
        def generate(source: S, c: FeatureContext) = f.generate(source, c)
      }
    }
  }

  object Metadata {
    sealed trait ValueType
    object ValueType {
      case object IntegralType extends ValueType
      case object DecimalType  extends ValueType
      case object StringType   extends ValueType
    }

    implicit class AsHydroPsv[V <: Value](m: Metadata[_, V]) {
      def asHydroPsv: String = {
        val valueType = m.valueType match {
          case ValueType.IntegralType => "int"
          case ValueType.DecimalType  => "double"
          case ValueType.StringType   => "string"
        }
        val featureType = m.featureType match {
          case Type.Categorical => "categorical"
          case Type.Continuous  => "continuous"
        }
        List(m.namespace + "." + m.name, valueType, featureType).map(_.toLowerCase).mkString("|")
      }
    }
  }

  import Metadata.ValueType

  case class Metadata[S : TypeTag, +V <: Value : TypeTag](
    namespace:   Namespace,
    name:        Name,
    description: Description,
    featureType: Type
  )(implicit neq: V =:!= Nothing) {
    def valueType =
      typeOf[V] match {
        // Would be nice to get exhaustiveness checking here
        case t if t =:= typeOf[Value.Integral] => ValueType.IntegralType
        case t if t =:= typeOf[Value.Decimal] =>  ValueType.DecimalType
        case t if t =:= typeOf[Value.Str] =>      ValueType.StringType
      }

    def sourceTag: TypeTag[S] = implicitly
  }
}

import Feature._

abstract class Feature[S, +V <: Value](val metadata: Metadata[S, V]) {
  def generate(source:S, context: FeatureContext): Option[FeatureValue[V]]
}

case class FeatureValue[+V <: Value](
  entity:  EntityId,
  name:    Name,
  value:   V,
  time:    Time
)

object FeatureValue {
  implicit class AsEavt[V <: Value](fv: FeatureValue[V]) {
    def asEavt: (EntityId, Name, V, Time) = (fv.entity, fv.name, fv.value, fv.time)
  }
}
