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

import org.scalacheck.Arbitrary
import org.scalacheck.Prop._

import org.specs2.{ScalaCheck, Specification}

import scalaz.NonEmptyList
import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary

import commbank.coppersmith.AggregationFeature.BigDAverages._

object AggregationSpec extends Specification with ScalaCheck {
  def is =
    s2"""
  BigDAveragedValue plus returns expected values $bigDAveragedGroup
"""

  implicit def arbRestrictedBigDecimal: Arbitrary[BigDecimal] =
    Arbitrary(Arbitrary.arbBigDecimal.arbitrary.retryUntil(bd => {
      (bd.scale > -1000) && (bd.scale < 1000) &&
        (bd > BigDecimal("-1000000000000000")) && (bd < BigDecimal("1000000000000000"))
    }))

  // Test lists of lists to mock parallel execution
  def bigDAveragedGroup = forAll { nels: NonEmptyList[NonEmptyList[BigDecimal]] => {
    val bigDAvgValLists = nels.map(_.map(BigDAveragedValue(1L, _)))
    val bds = for (bds <- nels; bd <- bds) yield bd

    val expected = BigDAveragedValue(bds.size, bds.list.sum / bds.size)

    val aggregationResults = bigDAvgValLists.map(plus)
    val actual = plus(aggregationResults)

    actual.value must beCloseTo(expected.value +/- BigDecimal("0.0000000000000000001"))
  }}

  def plus(vals: NonEmptyList[BigDAveragedValue]) = {
    vals.tail.foldLeft(vals.head)((l, r) => BigDAveragedGroup.plus(l, r))
  }

}
