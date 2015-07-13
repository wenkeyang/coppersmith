package au.com.cba.omnia.dataproducts.features

object Feature {
  type Namespace = String
  type Name      = String
  type EntityId  = String
  type Time      = String

  sealed trait Type
  object Type {
    case object Categorical extends Type
    case object Continuous  extends Type
  }

  sealed trait ValueType
  object ValueType {
    case object Integral extends ValueType
    case object Decimal  extends ValueType
    case object Str      extends ValueType
  }
}

import Feature._

case class FeatureMetadata(namespace: Namespace, name: Name, valueType: ValueType, featureType: Type)

trait Feature[S] {
  type V // Type of value

  def generate(source:S): Option[FeatureValue[S, V]]

  // Metadata
  def namespace:   Feature.Namespace
  def name:        Feature.Name
  def valueType:   Feature.ValueType
  def featureType: Feature.Type
}

case class FeatureValue[S, V](
  feature: Feature[S],
  entity:  EntityId,
  value:   V,
  time:    Time
)
