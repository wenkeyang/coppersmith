package examples

import commbank.coppersmith._
import Feature._
import Type._
/**
 * An example of defining metadata for features that are defined elsewhere.
 */
object LegacyFeatureSetExample extends MetadataSet {
  val legacyFeature1 = FeatureMetadata[Value.Str]    ("cep_features", "lgc_ftr_1", "A string feature", Categorical)
  val legacyFeature2 = FeatureMetadata[Value.Decimal]("cep_features", "lgc_ftr_2", "A decimal feature", Continuous)

  def metadata = List(legacyFeature1, legacyFeature2)
}
