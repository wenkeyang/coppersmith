package examples

import commbank.coppersmith._
import Feature._
import Type._

import commbank.coppersmith.example.thrift.Customer

/**
 * An example of defining metadata for features that are defined elsewhere.
 */
object LegacyFeatureSetExample extends MetadataSet[Customer] {
  val legacyFeature1 = Metadata[Customer, Value.Str](
    "cep_features", "lgc_ftr_1", "A string feature", Nominal
  )
  val legacyFeature2 = Metadata[Customer, Value.Decimal](
    "cep_features", "lgc_ftr_2", "A decimal feature", Continuous
  )

  def metadata = List(legacyFeature1, legacyFeature2)

  def main(args: Array[String]): Unit = {
    import MetadataOutput._

    println(metadataString(LegacyFeatureSetExample, LuaTable))
  }
}
