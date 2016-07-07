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

import commbank.coppersmith.util.{Timestamp, Datestamp}
import org.joda.time.DateTime
import org.scalacheck._, Prop.forAll

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
        Metadata[Customer, Integral]     (namespace, name, desc, fType).valueType must_== IntegralType
      case Decimal(_) =>
        Metadata[Customer, Decimal]      (namespace, name, desc, fType).valueType must_== DecimalType
      case FloatingPoint(_) =>
        Metadata[Customer, FloatingPoint](namespace, name, desc, fType).valueType must_== FloatingPointType
      case Str(_) =>
        Metadata[Customer, Str]          (namespace, name, desc, fType).valueType must_== StringType
      case Bool(_) =>
        Metadata[Customer, Bool]         (namespace, name, desc, fType).valueType must_== BoolType
      case Date(_) =>
        Metadata[Customer, Date]         (namespace, name, desc, fType).valueType must_== DateType
      case Time(_) =>
        Metadata[Customer, Time]         (namespace, name, desc, fType).valueType must_== TimeType
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

  implicit def arbStrs: Arbitrary[NonEmptyList[Str]] = NonEmptyListArbitrary(Arbitrary(strValueGen))

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

  def widestStr = forAll { (vals: NonEmptyList[Str]) => {
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
    Floating point features convert to continuous and to categorical  $floatingPointConversions
    String features cannot convert to continuous  $stringConversions
    Bool features only convert to nominal  $boolConversions
    Date features only convert to instant  $dateConversions
    Time features only convert to instant  $timeConversions
"""

  def integralConversions = {
    val feature = Patterns.general[Customer, Value.Integral, Value.Integral](
      "ns", "name", "Desc", Type.Nominal, _.id, c => Some(c.age), None
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
      "ns", "name", "Description", Type.Ordinal, _.id, c => Some(BigDecimal(c.age)), None
    )
    Seq(
      feature.metadata.featureType === Type.Ordinal,
      feature.as(Continuous).metadata.featureType === Type.Continuous,
      feature.as(Ordinal).metadata.featureType === Type.Ordinal,
      feature.as(Ordinal).as(Continuous).metadata.featureType === Type.Continuous
    )
  }

  def floatingPointConversions = {
    val feature = Patterns.general[Customer, Value.FloatingPoint, Value.FloatingPoint](
      "ns", "name", "Description", Type.Ordinal, _.id, c => Some(c.age.toDouble), None
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
      "ns", "name", "Description", Type.Nominal, _.id, c => Some(c.name), None
    )

    Seq(
      feature.metadata.featureType === Type.Nominal,
      typecheck("feature.as(Continuous)") must not succeed
    )
  }

  def boolConversions = {
    val feature = Patterns.general[Customer, Value.Bool, Value.Bool](
      "ns", "name", "Desc", Type.Nominal, _.id, c => Some(c.age > 20), None
    )

    Seq(
      feature.metadata.featureType === Type.Nominal,
      typecheck("feature.as(Continuous)") must not succeed
    )
  }

  def dateConversions = {
    val feature = Patterns.general[Customer, Value.Date, Value.Date](
      "ns", "name", "Description", Type.Instant, _.id, c => Some(Datestamp.unsafeParse(new DateTime(c.time).toString("yyyy-MM-dd"))), None
    )

    Seq(
      feature.metadata.featureType === Type.Instant,
      typecheck("feature.as(Continuous)") must not succeed,
      typecheck("feature.as(Ordinal)") must not succeed
    )
  }

  def timeConversions = {
    val feature = Patterns.general[Customer, Value.Time, Value.Time](
      "ns", "name", "Description", Type.Instant, _.id, c => Some(Timestamp(c.time, Some((0,0)))), None
    )

    Seq(
      feature.metadata.featureType === Type.Instant,
      typecheck("feature.as(Continuous)") must not succeed,
      typecheck("feature.as(Ordinal)") must not succeed
    )
  }
}
