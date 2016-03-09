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
        JsonObject
      )
    )
  }
}
