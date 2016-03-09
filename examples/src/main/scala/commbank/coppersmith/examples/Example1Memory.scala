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

package commbank.coppersmith.examples

import commbank.coppersmith.Feature.Value.Str
import commbank.coppersmith.Feature._
import commbank.coppersmith.PivotMacro._
import commbank.coppersmith.{lift => _, _}
import commbank.coppersmith.examples.thrift.Customer
import commbank.coppersmith.lift.memory._
import org.joda.time.DateTime


object Example1Memory {
  val pivoted = pivotThrift[Customer]("namespace", _.id)
  val pivotedAsFeatureSet: PivotFeatureSet[Customer] = pivoted
  val acct: Feature[Customer, Value.Str] = pivoted.acct
  val cat: Feature[Customer, Value.Str] = pivoted.cat
  val balance: Feature[Customer, Value.Integral] = pivoted.balance

  def main(args: Array[String]) = {
    val c1 = Customer(
      id = "",
      acct = "123",
      cat = "333",
      subCat = "444",
      balance = 100,
      effectiveDate = "01022001",
      dob = "10091988"
    )

    val c2 = Customer(
      id = "",
      acct = "124",
      cat = "333",
      subCat = "444",
      balance = 100,
      effectiveDate = "01022001",
      dob = "11101999"
    )

    val result: List[FeatureValue[Str]] = lift(acct)(List(c1, c2))
  }

}
