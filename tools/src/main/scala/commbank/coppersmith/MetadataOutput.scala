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

import argonaut._
import commbank.coppersmith.Feature.Conforms._

import commbank.coppersmith.Feature.Metadata._
import commbank.coppersmith.Feature.Value._
import commbank.coppersmith.Feature.{Value, _}
import commbank.coppersmith.tools.json._

object MetadataOutput {
  sealed trait MetadataOut {
    type OutType
    // Existential types caused pain here. The source and value types are actually
    // already encoded in the features, so just going to the top type parameter helps
    // things here.
    def doOutput(metadataSets: List[MetadataSet[Any]], conforms: Set[Conforms[Type, Value]]): OutType
    def stringify(o: OutType): String
  }

  object Psv extends MetadataOut {
    type OutType = String

    val singleItem = (md: Metadata[_, Value], oConforms: Option[Conforms[Type, Value]]) => {
      val valueType = psvValueTypeToString(md.valueType)
      val featureType = psvFeatureTypeToString(md.featureType)
      List(md.namespace + "." + md.name, valueType, featureType).map(_.toLowerCase).mkString("|")
    }

    def doOutput(metadataSets: List[MetadataSet[Any]], allConforms: Set[Conforms[Type, Value]]) = {
      val metadataWithConforms = metadataSets.flatMap(_.metadata).map(m => (m, allConforms.find(c => conforms_?(c, m))))
      metadataWithConforms.map(singleItem.tupled).mkString("\n")
    }

    def stringify(o: String) = o
  }

  object Json0 extends MetadataOut {
    type OutType = Json

    val singleItem = (md: Metadata[_, Value], oConforms: Option[Conforms[_, _]]) => {
      FeatureMetadataV0(
        namespace = md.namespace,
        name = md.name,
        description = md.description,
        source = md.sourceType.toString,
        typesConform = oConforms.isDefined,
        valueType = genericValueTypeToString(md.valueType),
        featureType = genericFeatureTypeToString(md.featureType),
        range = genericRangeToJson(md.valueRange))
    }


    def doOutput(metadataSets: List[MetadataSet[Any]],
                 allConforms: Set[Conforms[Type,Value]]): Json = {
      val metadataWithConforms: List[(Metadata[Any, Value], Option[Conforms[Type, Value]])] =
        metadataSets.flatMap(_.metadata).map { m =>
          (m, allConforms.find(c => conforms_?(c, m)))
        }.toList
      val features = metadataWithConforms.map (singleItem.tupled)
      MetadataJsonV0.write(MetadataJsonV0(features))
    }

    def stringify(o: Json) = o.spaces2
  }


  object Json1 extends MetadataOut {
    type OutType = Json

    val singleItem = (md: Metadata[_, Value], oConforms: Option[Conforms[_, _]]) => {
      FeatureMetadataV1(
        namespace = md.namespace,
        name = md.name,
        description = md.description,
        sourceType = md.sourceType.toString,
        typesConform = oConforms.isDefined,
        valueType = genericValueTypeToString(md.valueType),
        featureType = genericFeatureTypeToString(md.featureType),
        range = genericRangeToJson(md.valueRange))
    }


    def doOutput(metadataSets: List[MetadataSet[Any]], allConforms: Set[Conforms[Type,Value]]) = {
      val setJsons = metadataSets.map { ms =>
        val name: String = ms.name
        val metadataWithConforms: List[(Metadata[Any, Value], Option[Conforms[Type, Value]])] = ms.metadata.map(m => (m, allConforms.find(c => conforms_?(c, m)))).toList
        FeatureSetMetadataV1(name, metadataWithConforms.map (singleItem.tupled))
      }
      MetadataJsonV1.write(MetadataJsonV1(1, setJsons))
    }

    def stringify(o: Json) = o.spaces2
  }

  private def psvValueTypeToString(v: ValueType) = v match {
    case ValueType.IntegralType      => "int"
    case ValueType.DecimalType       => "bigdecimal"
    case ValueType.FloatingPointType => "double"
    case ValueType.StringType        => "string"
    case ValueType.DateType          => "date"
    case ValueType.TimeType          => "datetime"
    case ValueType.BoolType          => "bool"
  }

  private def psvFeatureTypeToString(f: Feature.Type) = f match {
    case t : Type.Categorical => "categorical"
    case t : Type.Numeric     => "continuous"
    case Type.Instant         => "datetime"
  }

  private def genericFeatureTypeToString(f: Feature.Type) = f.toString.toLowerCase

  private def genericValueTypeToString(v: ValueType) = v.toString.replace("Type", "").toLowerCase

  def genericValueToString(v: Value): Option[String] = (v match {
    case Integral(v)      => v.map(_.toString)
    case Decimal(v)       => v.map(_.toString)
    case FloatingPoint(v) => v.map(_.toString)
    case Str(v)           => v
    case Date(v)          => v.map(_.toString)
    case Time(v)          => v.map(_.toString)
    case Bool(v)          => v.map(_.toString)	
  })

  private def genericRangeToJson(r: Option[Value.Range[Value]]): Option[RangeV0] = r map {
    case Value.MinMaxRange(min, max) => NumericRangeV0(genericValueToString(min), genericValueToString(max))
    case Value.SetRange(set)         => SetRangeV0(set.map(genericValueToString).toList)
  }
}
