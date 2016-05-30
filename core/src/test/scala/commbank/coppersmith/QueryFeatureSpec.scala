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

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean

import org.scalacheck.Prop.forAll

import org.specs2._

import Feature._, Value._

import Arbitraries._

import commbank.coppersmith.test.thrift.Customer

/* More of an integration test based on a semi-realistic example. Individual feature components
 * are tested in GeneralFeatureSpec.
 */
object QueryFeatureSetSpec extends Specification with ScalaCheck { def is = s2"""
  QueryFeatureSet - Test an example set of features based on querying records
  ===========
  An example feature set
    must generate expected metadata       $generateMetadata
    must generate expected feature values $generateFeatureValues
"""

  import Type._

  object CustomerFeatureSet extends QueryFeatureSet[Customer, FloatingPoint] {
    val namespace   = "test.namespace"
    val featureType = Continuous

    def entity(c: Customer) = c.id
    def value(c: Customer)  = c.height

    def feature(name: String, humanDescription: String, condition: Customer => Boolean) = {
      queryFeature(name, humanDescription, condition)
    }

    type CustFeat = Feature[Customer, FloatingPoint]
    val youngHeight: CustFeat = feature("youngHeight", "Young Height",  _.age < 18)
    val midHeight:   CustFeat = feature("midHeight",   "Middle Height", c => Range(18 , 65).contains(c.age))
    val oldHeight:   CustFeat = feature("oldHeight",   "Old Height",    _.age >= 65)

    def features = List(youngHeight, midHeight, oldHeight)
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.metadata
    import CustomerFeatureSet.namespace

    metadata must_== List(
      Metadata[Customer, FloatingPoint](namespace, "youngHeight", "Young Height",  Continuous),
      Metadata[Customer, FloatingPoint](namespace, "midHeight",   "Middle Height", Continuous),
      Metadata[Customer, FloatingPoint](namespace, "oldHeight",   "Old Height",    Continuous)
    )
  }

  def generateFeatureValues = forAll { (c: Customer) => {
    val featureValues = CustomerFeatureSet.generate(c)

    import CustomerFeatureSet._
    val expectedFeature = if (c.age < 18) youngHeight else if (c.age >= 65) oldHeight else midHeight

    featureValues must_== List(
      FeatureValue[FloatingPoint](c.id, expectedFeature.metadata.name, c.height)
    )
  }}
}
