package commbank.coppersmith

import commbank.coppersmith.Feature.Value
import Feature._

object MetadataOutput {
  type MetadataPrinter = Metadata[_, Feature.Value] => String

  val HydroPsv: MetadataPrinter = md => md.asHydroPsv
  val Markdown: MetadataPrinter = md =>
    s"""
       |# ${md.name}
       |
       |## ${md.namespace}
       |
       | ${md.description}
       |
       | Source: ${md.sourceTag.tpe}
       |
       | Feature type: ${md.featureType}
       | Value Type: ${md.valueType}
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

  def metadataString[S, V <: Value](md: HasMetadata[S], printer: MetadataPrinter): String = {
    md.metadata.map(printer(_)).mkString("\n")
  }
}
