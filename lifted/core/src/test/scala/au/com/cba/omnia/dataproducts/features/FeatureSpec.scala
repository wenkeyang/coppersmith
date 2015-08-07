package au.com.cba.omnia.dataproducts.features

import org.scalacheck.Prop.forAll

import org.specs2._
import org.specs2.execute._, Typecheck._
import org.specs2.matcher.TypecheckMatchers._

import Feature._, Value._, Type._
import FeatureMetadata.ValueType._
import Arbitraries._

import au.com.cba.omnia.dataproducts.features.test.thrift.Customer

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
    val feature = Patterns.general[Customer, Value.Integral, Value.Integral](
      "ns", "name", Type.Categorical, _.id, c => Some(c.age), _.time
    )
    Seq(
      feature.metadata.featureType === Type.Categorical,
      feature.as(Continuous).metadata.featureType === Type.Continuous,
      feature.as(Categorical).metadata.featureType === Type.Categorical,
      feature.as(Categorical).as(Continuous).metadata.featureType === Type.Continuous
    )
  }

  def decimalConversions = {
    val feature = Patterns.general[Customer, Value.Decimal, Value.Decimal](
      "ns", "name", Type.Categorical, _.id, c => Some(c.age.toDouble), _.time
    )
    Seq(
      feature.metadata.featureType === Type.Categorical,
      feature.as(Continuous).metadata.featureType === Type.Continuous,
      feature.as(Categorical).metadata.featureType === Type.Categorical,
      feature.as(Categorical).as(Continuous).metadata.featureType === Type.Continuous
    )
  }

  def stringConversions = {
    val feature = Patterns.general[Customer, Value.Str, Value.Str](
      "ns", "name", Type.Categorical, _.id, c => Some(c.name), _.time
    )
    feature.metadata.featureType === Type.Categorical
    typecheck("feature.as(Continuous)") must not succeed
  }
}
