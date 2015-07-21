package au.com.cba.omnia.dataproducts.features

import au.com.cba.omnia.dataproducts.features.test.thrift.Customer
import org.scalacheck.Prop.forAll

import org.specs2._

import Feature._, Value._
import FeatureMetadata.ValueType._
import Arbitraries._
import shapeless.test.illTyped

object FeatureMetadataSpec extends Specification with ScalaCheck { def is = s2"""
  FeatureMetadata
  ===========
    All value types are covered $valueTypes
"""

  def valueTypes = forAll { (namespace: Namespace, name: Name, fType: Type, value: Value) => {
    /* Actual value is ignored - this primarily exists to make sure a compiler warning
     * is raised if a new value type is added without adding a test for it. Would also
     * good if warning was raised if instance wasn't added to Arbitraries.
     */
    value match {
      case Integral(_) => FeatureMetadata[Integral](namespace, name, fType).valueType must_== IntegralType
      case Decimal(_) =>  FeatureMetadata[Decimal] (namespace, name, fType).valueType must_== DecimalType
      case Str(_) =>      FeatureMetadata[Str]     (namespace, name, fType).valueType must_== StringType
    }
  }}
}

object FeatureTypeConversionsSpec extends Specification with ScalaCheck {
  def is = s2"""
  Feature conversions
  ===========
    Integral features convert to continuous and to categorical  $integralConversions
    Decimal features convert to continuous and to categorical  $decimalConversions
    String features cannot convert to continuous  $stringConversions
"""

  def integralConversions = {
    val feature = Patterns.general[Customer, Value.Integral, Value.Integral]("ns", "name", Type.Categorical, (c:Customer) => c._1, (c:Customer) => Some(c.age), (c:Customer) => 0)
    Seq(
      feature.metadata.featureType === Type.Categorical,
      feature.asContinuous.metadata.featureType === Type.Continuous,
      feature.asCategorical.metadata.featureType === Type.Categorical,
      feature.asCategorical.asContinuous.metadata.featureType === Type.Continuous
    )
  }

  def decimalConversions = {
    val feature = Patterns.general[Customer, Value.Decimal, Value.Decimal]("ns", "name", Type.Categorical, (c:Customer) => c._1, (c:Customer) => Some(c.age.toDouble), (c:Customer) => 0)
    Seq(
      feature.metadata.featureType === Type.Categorical,
      feature.asContinuous.metadata.featureType === Type.Continuous,
      feature.asCategorical.metadata.featureType === Type.Categorical,
      feature.asCategorical.asContinuous.metadata.featureType === Type.Continuous
    )
  }

  def stringConversions = {
    val feature = Patterns.general[Customer, Value.Str, Value.Str]("ns", "name", Type.Categorical, (c:Customer) => c._1, (c:Customer) => Some(c._1), (c:Customer) => 0)
    illTyped("feature.asContinuous")
    feature.metadata.featureType === Type.Categorical
  }
}
