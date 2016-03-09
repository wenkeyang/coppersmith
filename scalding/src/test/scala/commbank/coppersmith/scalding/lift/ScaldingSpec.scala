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

package commbank.coppersmith.scalding.lift

import com.twitter.scalding.typed._

import au.com.cba.omnia.thermometer.core.ThermometerSpec

import commbank.coppersmith.Feature.Value.Integral
import commbank.coppersmith.api._
import commbank.coppersmith.scalding.lift.scalding._
import commbank.coppersmith.test.thrift._

class ScaldingSpec extends ThermometerSpec {
  def is = s2"""
     Inner join $joins
     Left join  $leftJoins

     Lift feature     $liftFeature
     Lift feature set $liftFeatureSet
  """

  val cs = List(
    C(1, "1"),
    C(2, "2"),
    C(2, "22"),
    C(3, "333")
  )
  val ds = List(
    D(2, "2"),
    D(2, "22"),
    D(2, "222"),
    D(3, "333"),
    D(4, "4")
  )

  def joins = {
    val joinedPipe = liftJoin(Join[C].to[D].on(_.id, _.id))(IterablePipe(cs), IterablePipe(ds))
    val expected = List(
      C(2,"2") -> D(2, "2"),
      C(2,"2") -> D(2, "22"),
      C(2,"2") -> D(2, "222"),
      C(2,"22") -> D(2, "2"),
      C(2,"22") -> D(2, "22"),
      C(2,"22") -> D(2, "222"),
      C(3,"333") -> D(3, "333")
    )
    runsSuccessfully(joinedPipe) must_== expected
  }

  def leftJoins = {
    val expected = List(
      C(1, "1") -> None,
      C(2,"2") -> Some(D(2, "2")),
      C(2,"2") -> Some(D(2, "22")),
      C(2,"2") -> Some(D(2, "222")),
      C(2,"22") -> Some(D(2, "2")),
      C(2,"22") -> Some(D(2, "22")),
      C(2,"22") -> Some(D(2, "222")),
      C(3,"333") -> Some(D(3, "333"))
    )

    val joinedPipe = liftLeftJoin(Join.left[C].to[D].on(_.id, _.id))(IterablePipe(cs), IterablePipe(ds))
    runsSuccessfully(joinedPipe) must_== expected
  }

  object TestFeatureSet extends FeatureSet[Customer] {
    val source = From[Customer]()
    val entity = (cust: Customer) => cust.id
    val select = source.featureSetBuilder(namespace, entity)

    def namespace = "test"

    val ageFeature = select(_.age).asFeature(Continuous, "CUST_AGE", "Age fo the customer")

    val features = List(ageFeature)
  }

  val customerPipe = IterablePipe(List(
    Customer("1", "name1", 30, 2.0, None, 1L),
    Customer("2", "name2", 40, 1.5, None, 1L)
  ))

  def liftFeature = {
    val result = runsSuccessfully(scalding.lift(TestFeatureSet.ageFeature)(customerPipe))
    result === List(
      FeatureValue("1", "CUST_AGE", Integral(Some(30))),
      FeatureValue("2", "CUST_AGE", Integral(Some(40)))
    )
  }

  def liftFeatureSet = {
    val result = runsSuccessfully(scalding.lift(TestFeatureSet)(customerPipe))
    result === List(
      FeatureValue("1", "CUST_AGE", Integral(Some(30))),
      FeatureValue("2", "CUST_AGE", Integral(Some(40)))
    )
  }
}
