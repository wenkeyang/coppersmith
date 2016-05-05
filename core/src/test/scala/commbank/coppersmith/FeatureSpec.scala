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

import org.scalacheck._, Arbitrary.arbitrary, Prop.forAll

import org.specs2._
import org.specs2.execute._, Typecheck._
import org.specs2.matcher.TypecheckMatchers._

import scalaz.{Name => _, Value =>_, _}, Scalaz._
import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary

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

object FeatureValueRangeSpec extends Specification with ScalaCheck { def is = s2"""
  MinMaxRange
  ===========
    Contains same min max     $containsSameMinMax
    Contains min value        $containsMin
    Contains max value        $containsMax
    Contains mid-range value  $containsMid
    Excludes lower-min value  $excludesLowerMin
    Excludes higher-max value $excludesHigherMax
    Has no widest value size  $minMaxWidestNone

  SetRange
  ========
    Range values contained       $rangeValueContained
    Non-range values excluded    $nonRangeValuesExcluded
    No widest for non-Str values $nonStrWidest
    Widest Str wider than others $widestStr
"""

  // Generates values of the same subtype, but arbitrarily chooses the subtype to generate
  implicit def arbValues: Arbitrary[NonEmptyList[Value]] =
    Arbitrary(
      Gen.oneOf(
        NonEmptyListArbitrary(Arbitrary(decimalValueGen)).arbitrary,
        NonEmptyListArbitrary(Arbitrary(integralValueGen)).arbitrary,
        NonEmptyListArbitrary(Arbitrary(strValueGen)).arbitrary
      )
    )

  // Only for use with arbValues above - assumed values to be compared are of the same subtype
  implicit val valueOrder: Order[Value] =
    Order.order((a, b) => (a, b) match {
      case (Decimal(d1), Decimal(d2)) => d1.cmp(d2)
      case (Integral(i1), Integral(i2)) => i1.cmp(i2)
      case (Str(s1), Str(s2)) => s1.cmp(s2)
      case _ => sys.error("Assumption failed: Expected same value types from arbValues")
    })

  def containsSameMinMax = forAll { (value: Value) =>
    MinMaxRange(value, value).contains(value) must beTrue
  }

  def containsMin = forAll { (vals: NonEmptyList[Value]) =>
    val min = vals.minimum1
    MinMaxRange(min, vals.maximum1).contains(min) must beTrue
  }

  def containsMax = forAll { (vals: NonEmptyList[Value]) =>
    val max = vals.maximum1
    MinMaxRange(vals.minimum1, max).contains(max) must beTrue
  }

  def containsMid = testMinMidMax((min, mid, max) =>
    MinMaxRange(min, max).contains(mid) must beTrue
  )
  def excludesLowerMin = testMinMidMax((min, mid, max) =>
    MinMaxRange(mid, max).contains(min) must beFalse
  )
  def excludesHigherMax = testMinMidMax((min, mid, max) =>
    MinMaxRange(min, mid).contains(max) must beFalse
  )

  private def testMinMidMax(f: (Value, Value, Value) => Prop) = forAll {
    (vals: NonEmptyList[Value]) => (vals.list.toSet.size > 2) ==> {
      val min = vals.minimum1
      val max = vals.maximum1
      vals.list.find(v => min < v && v < max) match {
        case Some(mid) => f(min, mid, max)
        case _ => sys.error("Set > 2 assumed to have value between min & max")
      }
    }
  }

  def minMaxWidestNone = forAll { (vals: NonEmptyList[Value]) =>
    MinMaxRange(vals.minimum1, vals.maximum1).widestValueSize must beNone
  }

  def rangeValueContained = forAll { (idx: Int, vals: NonEmptyList[Value]) =>
    val randIndex = math.abs(idx % vals.size)
    SetRange(vals.list).contains(vals.list(randIndex)) must beTrue
  }

  def nonRangeValuesExcluded = forAll { (vals: NonEmptyList[Value]) =>
    val excluded = vals.head
    val range = vals.tail.toSet - excluded
    SetRange(range.toList).contains(excluded) must beFalse
  }

  def nonStrWidest = forAll { (vals: NonEmptyList[Value]) => !vals.head.isInstanceOf[Str] ==> {
    SetRange(vals.list).widestValueSize must beNone
  }}

  def widestStr = forAll { (vals: NonEmptyList[Value]) => vals.head.isInstanceOf[Str] ==> {
    val widest = vals.map {
      case Str(Some(s)) => s.length
      case _ => 0
    }.maximum1
    SetRange(vals.list).widestValueSize must beSome(widest)
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

object TypeInfoSpec extends Specification {
  def is = s2"""
    lists and strings don't have packages prepended $listsAndStrings
    def tuple2                                      $tuple2
    custom types have packages prepended            $custom
  """

  case class Internal[A]()

  import Metadata.TypeInfo

  def listsAndStrings = {
    TypeInfo[List[String]] === TypeInfo("List", List(TypeInfo("String", List())))
  }

  def custom = {
    TypeInfo[Internal[Int]] ===
      TypeInfo("commbank.coppersmith.TypeInfoSpec.Internal",
        List(TypeInfo("Int", List())))
  }

  def tuple2 = {
    TypeInfo[(String, Long)] ===
       TypeInfo("Tuple2",
         List(TypeInfo("String", List()), TypeInfo("Long", List())))
  }
}
