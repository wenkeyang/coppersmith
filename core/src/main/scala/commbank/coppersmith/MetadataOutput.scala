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

import commbank.coppersmith.Feature.Value
import Feature._
import Metadata._

import argonaut._, Argonaut._

object MetadataOutput {
  case class MetadataPrinter[O] (
    fn: (Metadata[_, Feature.Value], Option[Conforms[_, _]]) => O,
    combiner: List[O] => String)

  val defaultCombiner = (list: List[String]) => list.mkString("\n")

  private def psvValueTypeToString(v: ValueType) = v match {
    case ValueType.IntegralType      => "int"
    case ValueType.DecimalType       => "bigdecimal"
    case ValueType.FloatingPointType => "double"
    case ValueType.StringType        => "string"
    case ValueType.DateType          => "date"
    case ValueType.TimeType          => "datetime"
  }

  private def psvFeatureTypeToString(f: Feature.Type) = f match {
    case t : Type.Categorical => "categorical"
    case t : Type.Numeric     => "continuous"
    case Type.Instant         => "datetime"
  }

  private def genericFeatureTypeToString(f: Feature.Type) = f.toString.toLowerCase

  private def genericValueTypeToString(v: ValueType) = v.toString.replace("Type", "").toLowerCase

  val Psv: MetadataPrinter[String] = MetadataPrinter[String]((m, _) => {
      val valueType = psvValueTypeToString(m.valueType)
      val featureType = psvFeatureTypeToString(m.featureType)
      List(m.namespace + "." + m.name, valueType, featureType).map(_.toLowerCase).mkString("|")
  }, defaultCombiner)


  val JsonObject: MetadataPrinter[Json] = MetadataPrinter((md, oConforms) => {
    Json(
      "name" -> jString(md.name),
      "namespace" -> jString(md.namespace),
      "description" -> jString(md.description),
      "source" -> jString(md.sourceType.toString),
      "featureType" -> jString(genericFeatureTypeToString(md.featureType)),
      "valueType" -> jString(genericValueTypeToString(md.valueType)),
      "typesConform" -> jBool(oConforms.isDefined)
    )
  }, lst => jArrayElements(lst: _*).nospaces)


  trait HasMetadata[S] {
    def metadata: Iterable[Metadata[S, Value]]
  }

  implicit def fromFeature[S, V <: Value](f:Feature[S, V]): HasMetadata[S] = new HasMetadata[S] {
    def metadata: Iterable[Metadata[S, V]] = Seq(f.metadata)
  }

  implicit def fromMetadataSet[S](mds: MetadataSet[S]) = new HasMetadata[S] {
    def metadata = mds.metadata
  }

  def metadataString[S, O](
    metadata: List[(Metadata[S, Feature.Value], Option[Conforms[_, _]])],
    printer: MetadataPrinter[O]
  ): String = {
    printer.combiner(metadata.map(printer.fn.tupled))
  }
}
