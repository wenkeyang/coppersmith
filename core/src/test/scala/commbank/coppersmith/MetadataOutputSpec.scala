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

import commbank.coppersmith.Feature.Type._
import commbank.coppersmith.Feature.Value._
import commbank.coppersmith.Feature._
import commbank.coppersmith.test.thrift.Customer
import org.scalacheck.Prop._
import org.specs2.matcher.JsonMatchers
import org.specs2.{ScalaCheck, Specification}


import Arbitraries._

object MetadataOutputSpec extends Specification with ScalaCheck with JsonMatchers { def is = s2"""
  Psv creates expected metadata $psv
  Json creates expected metadata $json
  List produces valid json $listProducesValidJson
"""

  def psv = forAll { (namespace: Namespace, name: Name, desc: Description, fType: Type, value: Value) => {
    val (metadata, expectedValueType) = value match {
      case Integral(_)      => (Metadata[Customer, Integral]     (namespace, name, desc, fType), "int")
      case Decimal(_)       => (Metadata[Customer, Decimal]      (namespace, name, desc, fType), "bigdecimal")
      case FloatingPoint(_) => (Metadata[Customer, FloatingPoint](namespace, name, desc, fType), "double")
      case Str(_)           => (Metadata[Customer, Str]          (namespace, name, desc, fType), "string")
      case Date(_)          => (Metadata[Customer, Date]         (namespace, name, desc, fType), "date")
      case Time(_)          => (Metadata[Customer, Time]         (namespace, name, desc, fType), "datetime")
    }

    val expectedFeatureType = fType match {
      case n : Numeric    => "continuous"
      case c: Categorical => "categorical"
      case Instant        => "datetime"
    }

    val psvMetadata = MetadataOutput.Psv.fn(metadata, None)

    psvMetadata must_==
      s"${namespace.toLowerCase}.${name.toLowerCase}|$expectedValueType|$expectedFeatureType"
  }}

  def json = forAll { (namespace: Namespace, name: Name, desc: Description, fType: Type, value: Value) => {
    val (metadata, expectedValueType) = value match {
      case Integral(_)      => (Metadata[Customer, Integral]     (namespace, name, desc, fType), "integral")
      case Decimal(_)       => (Metadata[Customer, Decimal]      (namespace, name, desc, fType), "decimal")
      case FloatingPoint(_) => (Metadata[Customer, FloatingPoint](namespace, name, desc, fType), "floatingpoint")
      case Str(_)           => (Metadata[Customer, Str]          (namespace, name, desc, fType), "string")
      case Date(_)          => (Metadata[Customer, Date]         (namespace, name, desc, fType), "date")
      case Time(_)          => (Metadata[Customer, Time]         (namespace, name, desc, fType), "time")
    }

    val expectedFeatureType = fType.toString.toLowerCase
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

    val jsonOutput = MetadataOutput.JsonObject.fn(metadata, oConforms).nospaces
    Seq(
      jsonOutput must /("name" -> metadata.name),
      jsonOutput must /("namespace" -> metadata.namespace),
      jsonOutput must /("description" -> metadata.description),
      jsonOutput must /("source" -> metadata.sourceType.toString),
      jsonOutput must /("featureType" -> expectedFeatureType),
      jsonOutput must /("valueType" -> expectedValueType),
      jsonOutput must /("typesConform" -> expectedTypesConform)
    )
  }}

  def listProducesValidJson = {
    import MetadataOutput._
    val metadataList = List(
      Metadata[Customer, Integral]("ns", "feature1", "feature1", Discrete),
      Metadata[Customer, Str]("ns", "feature2", "feature2", Discrete)
    )

    val generatedJson = JsonObject.combiner(metadataList.map(md => JsonObject.fn(md, None)))
    argonaut.Parse.parse(generatedJson).isRight === true
  }
}
