package commbank.coppersmith

import commbank.coppersmith.Feature.Value

object MetadataOutput {
  type MetadataPrinter = FeatureMetadata[Feature.Value] => String

  val HydroPsv: MetadataPrinter = md => md.asHydroPsv
  val Markdown: MetadataPrinter = md =>
    s"""
       |# ${md.name}
       |
       |## ${md.namespace}
       |
       | ${md.description}
       |
       | Feature type: ${md.featureType}
       | Value Type: ${md.valueType}
       |
       | ${md.namespace}
     """.stripMargin
  trait HasMetadata {
    def metadata: Iterable[FeatureMetadata[Value]]
  }

  implicit def fromFeature[S, V <: Value](f:Feature[S, V]): HasMetadata = new HasMetadata {
    def metadata: Iterable[FeatureMetadata[V]] = Seq(f.metadata)
  }

  implicit def fromFeatureSet[S](f:FeatureSet[S]): HasMetadata = new HasMetadata {
    def metadata: Iterable[FeatureMetadata[Feature.Value]] = f.features.map(_.metadata)
  }

  implicit def fromMetadataSet(mds: MetadataSet) = new HasMetadata {
    def metadata = mds.metadata
  }

  def metadataString[V <: Value](md: HasMetadata, printer: MetadataPrinter): String = {
    md.metadata.map(printer(_)).mkString("\n")
  }

}
