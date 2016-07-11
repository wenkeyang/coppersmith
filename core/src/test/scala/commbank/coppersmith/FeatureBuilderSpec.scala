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

import commbank.coppersmith.util.Timestamp
import org.joda.time.DateTime
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen

import org.specs2._
import org.specs2.matcher.Matcher

import scalaz.NonEmptyList
import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary
import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean
import scalaz.syntax.std.list.ToListOpsFromList

import Feature._, Value._, Type._
import FeatureBuilderSource.fromFS
import Arbitraries._
import commbank.coppersmith.test.thrift.{Account, Customer}

object SelectFeatureSetSpec extends Specification with ScalaCheck { def is = s2"""
  SelectFeatureSet - Test an example set of features based on selecting fields
  ===========
  An example feature set
    must generate expected metadata       $generateMetadata
    must generate expected feature values $generateFeatureValues
"""

  object CustomerFeatureSet extends FeatureSet[Customer] {
    val namespace           = "test.namespace"
    def entity(c: Customer) = c.id
    def time(c: Customer, ctx: FeatureContext)   = c.time

    val source  = From[Customer]()
    val builder = source.featureSetBuilder(namespace, entity)
    val select  = builder

    type CF = Feature[Customer, Value]
    val age:       CF = select(_.age)                       .asFeature(Ordinal,    "age",       "Age")
    val tallAge:   CF = select(_.age).where(_.height > 2.0) .asFeature(Continuous, "tallAge",   "Tall Age")
    val oldHeight: CF = select(_.height).where(_.age > 65)  .asFeature(Continuous, "oldHeight", "Old Height")
    val time:      CF = select(c => Timestamp(c.time, None)).asFeature(Instant,    "time",      "Time")

    val credit:    CF = builder.map(c => (c, c.credit)).collect {
                          case (c, Some(score)) => (c, score)
                        }.select(_._2).asFeature(Continuous, "credit", "Credit Score")

    val altCredit: CF = builder.map(_.credit).collect {
                          case Some(score) => score
                        }.asFeature(Continuous, "altCredit", "Alternate Impl")

    def features = List(age, tallAge, oldHeight, time, credit, altCredit)
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.metadata
    import CustomerFeatureSet.namespace

    metadata must_== List(
      Metadata[Customer, Integral]     (namespace, "age",       "Age",            Ordinal),
      Metadata[Customer, Integral]     (namespace, "tallAge",   "Tall Age",       Continuous),
      Metadata[Customer, FloatingPoint](namespace, "oldHeight", "Old Height",     Continuous),
      Metadata[Customer, Time]         (namespace, "time",      "Time",           Instant),
      Metadata[Customer, FloatingPoint](namespace, "credit",    "Credit Score",   Continuous),
      Metadata[Customer, FloatingPoint](namespace, "altCredit", "Alternate Impl", Continuous)
    )
  }

  def generateFeatureValues = forAll { (c: Customer) => {
    val featureValues = CustomerFeatureSet.generate(c)

    val expectTallAge   = c.height > 2.0
    val expectOldHeight = c.age > 65
    val expectCredit    = c.credit.isDefined

    featureValues must_== List(
                        Some(FeatureValue[Integral]     (c.id, "age",       c.age)),
      expectTallAge.option(  FeatureValue[Integral]     (c.id, "tallAge",   c.age)),
      expectOldHeight.option(FeatureValue[FloatingPoint](c.id, "oldHeight", c.height)),
                        Some(FeatureValue[Time]         (c.id, "time",      Timestamp(c.time, None))),
      expectCredit.option(   FeatureValue[FloatingPoint](c.id, "credit",    c.credit)),
      expectCredit.option(   FeatureValue[FloatingPoint](c.id, "altCredit", c.credit))
    ).flatten
  }}
}

object AggregationFeatureSetSpec extends Specification with ScalaCheck { def is = s2"""
  AggregationFeatureSet - Test an example set of features based on aggregating records
  ===========
  An example feature set
    must generate expected metadata       $generateMetadata
    must generate expected feature values $generateFeatureValues
  The avg aggregator
    must generate expected values         $generateAvgValues
"""

  object CustomerFeatureSet extends AggregationFeatureSet[Customer] {
    val namespace           = "test.namespace"
    def entity(c: Customer) = c.id
    def time(c: Customer, ctx: FeatureContext)   = ctx.generationTime.getMillis

    val source  = From[Customer]()
    val builder = source.featureSetBuilder(namespace, entity)
    val select: FeatureSetBuilder[Customer, Customer]  = builder

    type CAF = AggregationFeature[Customer, Customer, _, Value]

    def bigD(f: Customer => Double): (Customer => BigDecimal) = c => BigDecimal(f(c))

    val sizeF:    CAF = select(size)                      .asFeature(Discrete,   "size",    "Agg feature")
    val bigSizeF: CAF = select(size).having(_ > 5)        .asFeature(Discrete,   "sizeB",   "Agg feature")
    val countF:   CAF = select(count(where = _.age >= 18)).asFeature(Continuous, "count",   "Agg feature")
    val sumF:     CAF = select(sum(_.height))             .asFeature(Continuous, "sum",     "Agg feature")
    val maxF:     CAF = select(max(_.age))                .asFeature(Continuous, "max",     "Agg feature")
    val minF:     CAF = select(min(_.height))             .asFeature(Continuous, "min",     "Agg feature")
    val maxByF:   CAF = select(maxBy(_.id)(_.height))     .asFeature(Continuous, "maxBy",   "Agg feature")
    val minByF:   CAF = select(minBy(_.id)(_.age))        .asFeature(Continuous, "minBy",   "Agg feature")
    val avgF:     CAF = select(avg(_.age.toDouble))       .asFeature(Continuous, "avg",     "Agg feature")
    val sumBigDF: CAF = select(sum(bigD(_.height)))       .asFeature(Continuous, "sumBigD", "Agg feature")
    val maxBigDF: CAF = select(max(bigD(_.age)))          .asFeature(Continuous, "maxBigD", "Agg feature")
    val avgBigDF: CAF = select(avgBigDec(_.age))          .asFeature(Continuous, "avgBigD", "Agg feature")
    val ucbF:     CAF = select(uniqueCountBy(_.age % 10)) .asFeature(Continuous, "ucb",     "Agg feature")

    import com.twitter.algebird.Aggregator

    val collectF: AggregationFeature[Customer, Double, _, Value] =
      builder.map(c => (c, c.credit)).collect {
        case (c, Some(credit)) => credit
      }.select(Aggregator.min[Double]).asFeature(Continuous, "collect", "Agg feature")

    def aggregationFeatures = List(
      sizeF,
      bigSizeF,
      countF,
      sumF,
      maxF,
      minF,
      maxByF,
      minByF,
      avgF,
      sumBigDF,
      maxBigDF,
      avgBigDF,
      ucbF,
      collectF
    )
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.metadata
    import CustomerFeatureSet.namespace

    metadata must_== List(
      Metadata[(EntityId, Iterable[Customer]), Integral]     (namespace, "size",    "Agg feature", Discrete),
      Metadata[(EntityId, Iterable[Customer]), Integral]     (namespace, "sizeB",   "Agg feature", Discrete),
      Metadata[(EntityId, Iterable[Customer]), Integral]     (namespace, "count",   "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), FloatingPoint](namespace, "sum",     "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), Integral]     (namespace, "max",     "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), FloatingPoint](namespace, "min",     "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), FloatingPoint](namespace, "maxBy",   "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), Integral]     (namespace, "minBy",   "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), FloatingPoint](namespace, "avg",     "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), Decimal]      (namespace, "sumBigD", "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), Decimal]      (namespace, "maxBigD", "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), Decimal]      (namespace, "avgBigD", "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), Integral]     (namespace, "ucb",     "Agg feature", Continuous),
      Metadata[(EntityId, Iterable[Customer]), FloatingPoint](namespace, "collect", "Agg feature", Continuous)
    )
  }

  def generateFeatureValues = forAll { (cs: NonEmptyList[Customer], dateTime: DateTime) => {
    val featureValues = CustomerFeatureSet.generate((cs.head.id, cs.list))

    val eavtValues = featureValues.map { fv => fv.asEavt(dateTime.getMillis) }.toList

    val c           = cs.head
    val heights     = cs.map(_.height).list
    val ages        = cs.map(_.age).list
    val groupedAges = cs.map(_.age).list.groupBy(_ % 10)
    val credit      = cs.map(_.credit).list.collect { case Some(c) => c }.toNel.map(_.list.min)
    val time        = dateTime.getMillis
    val maxCById    = cs.list.maxBy(_.id)
    val minCById    = cs.list.minBy(_.id)
    val expectBig   = cs.size > 5

    val expected = List(
                  Some((c.id, "size",    cs.size:                            Integral,      time)),
      expectBig.option((c.id, "sizeB",   cs.size:                            Integral,      time)),
                  Some((c.id, "count",   ages.filter(_ >= 18).size:          Integral,      time)),
                  Some((c.id, "sum",     heights.sum:                        FloatingPoint, time)),
                  Some((c.id, "max",     ages.max:                           Integral,      time)),
                  Some((c.id, "min",     heights.min:                        FloatingPoint, time)),
                  Some((c.id, "maxBy",   maxCById.height:                    FloatingPoint, time)),
                  Some((c.id, "minBy",   minCById.age:                       Integral,      time)),
                  Some((c.id, "avg",     (ages.sum.toDouble / ages.size):    FloatingPoint, time)),
                  Some((c.id, "sumBigD", heights.map(BigDecimal(_)).sum:     Decimal,       time)),
                  Some((c.id, "maxBigD", ages.map(BigDecimal(_)).max:        Decimal,       time)),
                  Some((c.id, "avgBigD", (ages.sum / BigDecimal(ages.size)): Decimal,       time)),
                  Some((c.id, "ucb",     groupedAges.size:                   Integral,      time)),
      credit.map(d =>  (c.id, "collect", d:                                  FloatingPoint, time))
    ).flatten

    eavtValues.toList must matchEavts(expected)
  }}

  def matchEavts(expected: List[(EntityId, Name, Value, FeatureTime)])
      : Matcher[List[(EntityId, Name, Value, FeatureTime)]] =
    expected.contain(_.zip(===, ===, matchValue, ===))

  def matchValue(expected: Value): Matcher[Value] = expected match {
    case Integral(_) | Str(_) | FloatingPoint(None) | Decimal(None) | Bool(_) | Date(_) | Time(_) => be_===(expected)
    case FloatingPoint(Some(expectedDouble)) =>
      beCloseTo(expectedDouble +/- 0.000000000001) ^^ (
        (v: Value) => v match {
          case FloatingPoint(Some(actualDouble)) => actualDouble
          case _ => Double.NaN
        }
      )
    case Decimal(Some(expectedBigDecimal)) =>
      beCloseTo(expectedBigDecimal +/- BigDecimal("0.0000000000000000001")) ^^ (
        (v: Value) => v match {
          case Decimal(Some(actualBigDecimal)) => actualBigDecimal
          case _ => null
        }
      )
  }

  object AverageFeatureSet extends AggregationFeatureSet[Account] {
    val namespace                               = "test.namespace"
    def entity(a: Account)                      = a.id
    def time(a: Account, ctx: FeatureContext)   = ctx.generationTime.getMillis

    val source  = From[Account]()
    val builder = source.featureSetBuilder(namespace, entity)
    val select: FeatureSetBuilder[Account, Account] = builder

    type CAF      = AggregationFeature[Account, Account, _, Value]
    val avgF: CAF = select(avgBigDec(c => BigDecimal(c.balanceBigDecimal)))
      .asFeature(Continuous, "avg", "Agg feature")

    def aggregationFeatures = List(avgF)
  }

  def generateAvgValues = {
    testAverage(NonEmptyList("1000000000000.02", "0.02"), BigDecimal("500000000000.02"))
    testAverage(NonEmptyList("1200000000000.01", "1200000000000.03", "0.02"), BigDecimal("800000000000.02"))
  }

  def testAverage(vals: NonEmptyList[String], expected: BigDecimal) = {
    val as   = vals.map(v => Gen.resultOf(Account.apply _).sample.get.copy(balanceBigDecimal = v))
    val time = DateTime.now.getMillis

    val featureValues = AverageFeatureSet.generate((as.head.id, as.list))

    val eavtValues = featureValues.map { fv => fv.asEavt(time) }.toList

    eavtValues.toList must matchEavts(List((as.head.id, "avg", expected: Decimal, time)))
  }
}
