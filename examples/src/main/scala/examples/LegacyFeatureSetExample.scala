package examples

import commbank.coppersmith._
import Feature._
import Type._
import Patterns.empty
/**
 * An example of defining metadata for features that are defined elsewhere.
 */
object LegacyFeatureSetExample extends MetadataSet {
  val legacyFeature1 = empty(FeatureMetadata("cep_features", "lgc_ftr_1", "A string feature", Categorical))
  val legacyFeature2 = empty(FeatureMetadata("cep_features", "lgc_ftr_2", "A decimal feature", Continuous))

  def features = List(legacyFeature1, legacyFeature2)
}
