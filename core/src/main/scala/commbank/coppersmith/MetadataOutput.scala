package commbank.coppersmith

import commbank.coppersmith.Feature.Value
import Feature._
import Metadata._

object MetadataOutput {
  type MetadataPrinter = Metadata[_, Feature.Value] => String

  val HydroPsv: MetadataPrinter = m => {
      val valueType = m.valueType match {
        case ValueType.IntegralType => "int"
        case ValueType.DecimalType  => "double"
        case ValueType.StringType   => "string"
      }
      val featureType = m.featureType match {
        case t : Type.Categorical => "categorical"
        case t : Type.Numeric     => "continuous"   //Assumption hydro interprets all numeric as continuous
      }
      List(m.namespace + "." + m.name, valueType, featureType).map(_.toLowerCase).mkString("|")
  }

  val Lua: MetadataPrinter = md =>
    s""" {
       name = "${md.name}",
       namespace = "${md.namespace}",
       description = "${md.description}",
       source = "${md.sourceTag.tpe}",
       featureType = "${md.featureType}",
       valueType = "${md.valueType}"
     }
     """

  val Markdown: MetadataPrinter = md =>
    s"""
       |# ${md.name}
       |
       |Meta          Value
       |------------  ------------------------
       |Identifier    ${md.name}
       |Namespace     ${md.namespace}
       |Description   ${md.description}
       |Source        ${md.sourceTag.tpe}
       |Feature type  ${md.featureType}
       |Value Type    ${md.valueType}
       |
     """.stripMargin


  trait HasMetadata[S] {
    def metadata: Iterable[Metadata[S, Value]]
  }

  implicit def fromFeature[S, V <: Value](f:Feature[S, V]): HasMetadata[S] = new HasMetadata[S] {
    def metadata: Iterable[Metadata[S, V]] = Seq(f.metadata)
  }

  implicit def fromMetadataSet[S](mds: MetadataSet[S]) = new HasMetadata[S] {
    def metadata = mds.metadata
  }

  //TODO: leaky abstraction here because the way in which the strings are joined depends on the language itself. this one is lua
  def metadataString[S, V <: Value](md: HasMetadata[S], printer: MetadataPrinter): String = {
    s"{${md.metadata.map(printer(_)).mkString(",\n")}}"
  }
}
