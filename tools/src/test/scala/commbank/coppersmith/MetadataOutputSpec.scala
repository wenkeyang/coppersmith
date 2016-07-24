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

import argonaut.Argonaut._
import commbank.coppersmith.Feature.Type._
import commbank.coppersmith.Feature.Value._
import commbank.coppersmith.Feature._
import commbank.coppersmith.test.thrift.Customer
import org.specs2._
import org.scalacheck._, Prop.forAll, Arbitrary.arbitrary
import org.specs2.matcher.{JsonMatchers, Matcher}
import Arbitraries._
import commbank.coppersmith.tools.json._, CodecsV0._

object MetadataOutputSpec extends Specification with ScalaCheck with JsonMatchers { def is = s2"""
  GenericValueToString creates expected values $genericValueToString
  Psv creates expected metadata $psv
  Json creates expected metadata $json
"""

  def genericValueToString = forAll { (v: Value) => {
    val expected: Option[String] = (v match {
      case Integral(v)      => v.map(_.toString)
      case Decimal(v)       => v.map(_.toString)
      case FloatingPoint(v) => v.map(_.toString)
      case Str(v)           => v
      case Bool(v)          => v.map(_.toString)
      case Date(v)          => v.map(_.toString)
      case Time(v)          => v.map(_.toString)
    })
    MetadataOutput.genericValueToString(v) must_== expected
  }}

  def psv = forAll { (namespace: Namespace, name: Name, desc: Description, fType: Type, value: Value) => {
    val (metadata, expectedValueType) = value match {
      case Integral(_)      => (Metadata[Customer, Integral]     (namespace, name, desc, fType), "int")
      case Decimal(_)       => (Metadata[Customer, Decimal]      (namespace, name, desc, fType), "bigdecimal")
      case FloatingPoint(_) => (Metadata[Customer, FloatingPoint](namespace, name, desc, fType), "double")
      case Str(_)           => (Metadata[Customer, Str]          (namespace, name, desc, fType), "string")
      case Bool(_)          => (Metadata[Customer, Bool]         (namespace, name, desc, fType), "bool")
      case Date(_)          => (Metadata[Customer, Date]         (namespace, name, desc, fType), "date")
      case Time(_)          => (Metadata[Customer, Time]         (namespace, name, desc, fType), "datetime")
    }

    val expectedFeatureType = fType match {
      case n : Numeric    => "continuous"
      case c: Categorical => "categorical"
      case Instant        => "datetime"
    }

    val psvMetadata = MetadataOutput.Psv.singleItem(metadata, None)

    psvMetadata must_==
      s"${namespace.toLowerCase}.${name.toLowerCase}|$expectedValueType|$expectedFeatureType"
  }}

  def typeMatches(v: Value, r: Option[Range[Value]]): Boolean = {
    r match {
      case Some(MinMaxRange(min, _)) if min.getClass == v.getClass => true
      case Some(SetRange(vs)) if vs.nonEmpty && vs.head.getClass == v.getClass => true
      case None => true
      case _ => false
    }
  }

  implicit val arbValueRange: Arbitrary[(Value, Option[Range[Value]])] = Arbitrary((for {
    v <- arbitrary[Value]
    r <- arbitrary[Option[Range[Value]]]
  } yield (v, r)).retryUntil { case (v, r) => typeMatches(v, r) })

  def json = forAll { (namespace: Namespace, name: Name, desc: Description, fType: Type, vr: (Value, Option[Range[Value]])) => {
    val (value, range) = vr
    val (metadata, expectedValueType) = vr match {
      case (Integral(_), r)      => (Metadata[Customer, Integral]     (namespace, name, desc, fType, r.asInstanceOf[Option[Range[Integral]]]), "integral")
      case (Decimal(_), r)       => (Metadata[Customer, Decimal]      (namespace, name, desc, fType, r.asInstanceOf[Option[Range[Decimal]]]), "decimal")
      case (FloatingPoint(_), r) => (Metadata[Customer, FloatingPoint](namespace, name, desc, fType, r.asInstanceOf[Option[Range[FloatingPoint]]]), "floatingpoint")
      case (Str(_), r)           => (Metadata[Customer, Str]          (namespace, name, desc, fType, r.asInstanceOf[Option[Range[Str]]]), "string")
      case (Bool(_), r)          => (Metadata[Customer, Bool]         (namespace, name, desc, fType, r.asInstanceOf[Option[Range[Bool]]]), "bool")
      case (Date(_), r)          => (Metadata[Customer, Date]         (namespace, name, desc, fType, r.asInstanceOf[Option[Range[Date]]]), "date")
      case (Time(_), r)          => (Metadata[Customer, Time]         (namespace, name, desc, fType, r.asInstanceOf[Option[Range[Time]]]), "time")
    }

    val expectedFeatureType = fType.toString.toLowerCase

    def expectedValue(v: Value): String = {
      MetadataOutput.genericValueToString(v).getOrElse("null")
    }

    val matchExpectedRange: Matcher[String] = range match {
      case Some(MinMaxRange(min,max)) => /("range") /("min" -> expectedValue(min)) and
                                         /("range") /("max" -> expectedValue(max))
      case Some(SetRange(set))        => /("range").andHave(allOf(set.map(expectedValue).toList:_*))
      case None                       => /("range").negate
    }

    val oConforms = (fType, value) match {
      case (Nominal,    Str(_))      => Some(NominalStr)
      case (Ordinal,    Decimal(_))  => Some(OrdinalDecimal)
      case (Continuous, Decimal(_))  => Some(ContinuousDecimal)
      case (Ordinal,    Integral(_)) => Some(OrdinalIntegral)
      case (Continuous, Integral(_)) => Some(ContinuousIntegral)
      case (Discrete,   Integral(_)) => Some(DiscreteIntegral)
      case (Instant,    Date(_))     => Some(InstantDate)
      case (Instant,    Time(_))     => Some(InstantTime)
      case _                         => None
    }
    val expectedTypesConform = oConforms.isDefined

    val jsonOutput = MetadataOutput.Json0.singleItem(metadata, oConforms).asJson.nospaces
    Seq(
      jsonOutput must /("name" -> metadata.name),
      jsonOutput must /("namespace" -> metadata.namespace),
      jsonOutput must /("description" -> metadata.description),
      jsonOutput must /("source" -> metadata.sourceType.toString),
      jsonOutput must /("featureType" -> expectedFeatureType),
      jsonOutput must /("valueType" -> expectedValueType),
      jsonOutput must /("typesConform" -> expectedTypesConform),
      jsonOutput must matchExpectedRange
    )
  }}
}
