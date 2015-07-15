package au.com.cba.omnia.dataproducts.features

import org.scalacheck.Prop.forAll

import org.specs2._

import Feature._, Value._
import FeatureMetadata.ValueType._
import Arbitraries._

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
