package examples

import commbank.coppersmith.api._
import commbank.coppersmith.{Feature, MetadataOutput}

import commbank.coppersmith.examples.thrift.Customer

/**
 * An example of defining metadata for features that are defined elsewhere.
 */
object LegacyFeatureSetExample extends MetadataSet[Customer] {

  val legacyFeature1 = FeatureStub[Customer, Decimal].asFeatureMetadata(
    Nominal, "cep_features", "lgc_ftr_1", "A string feature")

  val legacyFeature2 = FeatureStub[Customer, Decimal].asFeatureMetadata(
    Continuous, "cep_features", "lgc_ftr_2", "A decimal feature"
  )

  def metadata = List(legacyFeature1, legacyFeature2)

  def main(args: Array[String]): Unit = {
    import Feature._, MetadataOutput._, Conforms.conforms_?
    val allConforms = List(
      NominalStr,
      OrdinalDecimal,
      ContinuousDecimal,
      OrdinalIntegral,
      ContinuousIntegral,
      DiscreteIntegral
    )

    println(
      metadataString(
        LegacyFeatureSetExample.metadata.map(m => (m, allConforms.find(conforms_?(_, m)))),
        LuaTable
      )
    )
  }
}
