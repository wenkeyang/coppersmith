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

package commbank.coppersmith.scalding

import org.joda.time.DateTime

import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean

import commbank.coppersmith.FeatureSetBuilder
import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.Arbitraries.CustomerAccounts
import commbank.coppersmith.scalding.EavtText.EavtEnc
import commbank.coppersmith.test.thrift.Account

object TestFeatureSets {
  object RegularFeatures extends FeatureSet[Account] {
    val namespace          = "test.namespace"
    def entity(a: Account) = a.id

    val source  = From[Account]()
    val builder = source.featureSetBuilder(namespace, entity)
    val select: FeatureSetBuilder[Account, Account] = builder

    type AF = Feature[Account, Value]

    val balanceF: AF = select(_.balance).asFeature(Continuous, "balance", "test")
    val ageF: AF = select.map(_.age).collect {
      case Some(age) => age
    }.asFeature(Continuous, "age", "test")

    def features = List(balanceF, ageF)

    def expectedFeatureValues(custAccts: CustomerAccounts, time: DateTime) = {
      custAccts.cas.flatMap(_.as.flatMap(acct => {
        val values = List(
          List(FeatureValue[FloatingPoint]   (acct.id, "balance", acct.balance)),
          acct.age.map(FeatureValue[Integral](acct.id, "age",     _)).toList
        ).flatten
        values.map(v => EavtEnc.encode((v, time.getMillis)))
      })).toList
    }
  }

  object AggregationFeatures extends AggregationFeatureSet[Account] {
    val namespace          = "test.namespace"
    def entity(a: Account) = a.customerId

    val source  = From[Account]()
    val builder = source.featureSetBuilder(namespace, entity)
    val select: FeatureSetBuilder[Account, Account]  = builder

    type AAF = AggregationFeature[Account, Account, _, Value]

    val sizeF:  AAF = select(size)             .asFeature(Continuous, "size",    "test")
    val sizeBF: AAF = select(size).having(_> 2).asFeature(Continuous, "sizeBig", "test")
    val minF:   AAF = select(min(_.balance))   .asFeature(Continuous, "min",     "test")

    import com.twitter.algebird.Aggregator

    val collectF: AggregationFeature[Account, Int, _, Value] =
      builder.map(_.age).collect {
        case Some(age) => age
      }.select(Aggregator.fromMonoid[Int]).asFeature(Continuous, "collect", "test")

    // Make sure reflection code in SimpleFeatureJobOps.Unjoiner works with empty source.
    // Note that it will never be in the expectedFeatureValues.
    val knownEmptyF: AggregationFeature[Account, Int, _, Value] =
      builder.map(_.age).collect {
        case Some(age) if false => age
      }.select(Aggregator.fromMonoid[Int]).asFeature(Continuous, "knownEmpty", "test")

    def aggregationFeatures = List(sizeF, sizeBF, minF, knownEmptyF, collectF)

    def expectedFeatureValues(custAccts: CustomerAccounts, time: DateTime) = {

      custAccts.cas.flatMap(cag => {
        val size       = cag.as.size
        val sizesOver2 = cag.as.size > 2
        val min        = cag.as.map(_.balance).min
        val ages       = cag.as.map(_.age).collect { case Some(age) => age }
        val collect    = ages.toNel.map(_.list.sum)
        val values     = List(
          Some(FeatureValue[Integral]              (cag.c.id, "size",    size)),
          sizesOver2.option((FeatureValue[Integral](cag.c.id, "sizeBig", size))),
          Some(FeatureValue[FloatingPoint]         (cag.c.id, "min",     min)),
          collect.map(FeatureValue[Integral]       (cag.c.id, "collect", _))
        ).flatten
        values.map(v => EavtEnc.encode((v, time.getMillis)))
      }).toList
    }
  }
}
