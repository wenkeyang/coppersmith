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

import org.scalacheck.Prop.forAll

import org.specs2._
import org.specs2.execute._, Typecheck._
import org.specs2.matcher.TypecheckMatchers._

import Feature._, Value._, Type._
import Metadata.ValueType._
import Arbitraries._

import test.thrift.Customer

object MetadataSpec extends Specification with ScalaCheck { def is = s2"""
  Metadata
  ===========
    All value types are covered $valueTypes
"""

  def valueTypes = forAll { (namespace: Namespace, name: Name, desc: String, fType: Type, value: Value) => {
    /* Actual value is ignored - this primarily exists to make sure a compiler warning
     * is raised if a new value type is added without adding a test for it. Would also
     * good if warning was raised if instance wasn't added to Arbitraries.
     */
    value match {
      case Integral(_) =>
        Metadata[Customer, Integral](namespace, name, desc, fType).valueType must_== IntegralType
      case Decimal(_) =>
        Metadata[Customer, Decimal] (namespace, name, desc, fType).valueType must_== DecimalType
      case Str(_) =>
        Metadata[Customer, Str]     (namespace, name, desc, fType).valueType.must_==(StringType)
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
      "ns", "name", "Desc", Type.Nominal, _.id, c => Some(c.age)
    )
    Seq(
      feature.metadata.featureType === Type.Nominal,
      feature.as(Continuous).metadata.featureType === Type.Continuous,
      feature.as(Ordinal).metadata.featureType === Type.Ordinal,
      feature.as(Ordinal).as(Continuous).metadata.featureType === Type.Continuous
    )
  }

  def decimalConversions = {
    val feature = Patterns.general[Customer, Value.Decimal, Value.Decimal](
      "ns", "name", "Description", Type.Ordinal, _.id, c => Some(c.age.toDouble)
    )
    Seq(
      feature.metadata.featureType === Type.Ordinal,
      feature.as(Continuous).metadata.featureType === Type.Continuous,
      feature.as(Ordinal).metadata.featureType === Type.Ordinal,
      feature.as(Ordinal).as(Continuous).metadata.featureType === Type.Continuous
    )
  }

  def stringConversions = {
    val feature = Patterns.general[Customer, Value.Str, Value.Str](
      "ns", "name", "Description", Type.Nominal, _.id, c => Some(c.name)
    )
    feature.metadata.featureType === Type.Nominal
    typecheck("feature.as(Continuous)") must not succeed
  }
}
