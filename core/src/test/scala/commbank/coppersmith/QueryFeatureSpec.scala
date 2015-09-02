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

  import Type.{Categorical, Continuous}

  object CustomerFeatureSet extends QueryFeatureSet[Customer, Decimal] {
    val namespace   = "test.namespace"
    val featureType = Continuous

    def entity(c: Customer) = c.id
    def value(c: Customer)  = c.height
    def time(c: Customer)   = c.time

    def feature(name: String, humanDescription: String, condition: Customer => Boolean) = {
      queryFeature(name, humanDescription, condition)
    }

    val youngHeight: Feature[Customer, Decimal] = feature("youngHeight", "Young Height", _.age < 18)
    val midHeight:   Feature[Customer, Decimal] = feature("midHeight",  "Middle Height", c => Range(18 , 65).contains(c.age))
    val oldHeight:   Feature[Customer, Decimal] = feature("oldHeight", "Old Height",  _.age >= 65)

    def features = List(youngHeight, midHeight, oldHeight)
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.metadata

    metadata must_== List(
      FeatureMetadata[Decimal](CustomerFeatureSet.namespace, "youngHeight", "Young Height", Continuous),
      FeatureMetadata[Decimal](CustomerFeatureSet.namespace, "midHeight",   "Middle Height", Continuous),
      FeatureMetadata[Decimal](CustomerFeatureSet.namespace, "oldHeight",  "Old Height", Continuous)
    )
  }

  def generateFeatureValues = forAll { (c: Customer) => {
    val featureValues = CustomerFeatureSet.generate(c)

    import CustomerFeatureSet._
    val expectedFeature = if (c.age < 18) youngHeight else if (c.age >= 65) oldHeight else midHeight

    featureValues must_== List(
      FeatureValue[Decimal](c.id, expectedFeature.metadata.name, c.height, c.time)
    )
  }}
}
