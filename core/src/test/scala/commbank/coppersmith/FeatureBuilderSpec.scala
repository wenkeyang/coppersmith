package commbank.coppersmith

import org.joda.time.DateTime
import org.scalacheck.Prop.forAll

import org.specs2._
import org.specs2.matcher.Matcher

import scalaz.NonEmptyList
import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary
import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean

import Feature._, Value._, Type._
import FeatureBuilderSource.fromFS
import Arbitraries._
import test.thrift.Customer

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

    val source = From[Customer]()
    val select = source.featureSetBuilder(namespace, entity)

    type CF = Feature[Customer, Value]
    val age:       CF = select(_.age)                      .asFeature(Ordinal,     "age",       "Age")
    val tallAge:   CF = select(_.age).where(_.height > 2.0).asFeature(Continuous,  "tallAge",   "Tall Age")
    val oldHeight: CF = select(_.height).where(_.age > 65) .asFeature(Continuous,  "oldHeight", "Old Height")

    def features = List(age, tallAge, oldHeight)
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.metadata
    import CustomerFeatureSet.namespace

    metadata must_== List(
      Metadata[Customer, Integral](namespace, "age",       "Age",        Ordinal),
      Metadata[Customer, Integral](namespace, "tallAge",   "Tall Age",   Continuous),
      Metadata[Customer, Decimal] (namespace, "oldHeight", "Old Height", Continuous)
    )
  }

  def generateFeatureValues = forAll { (c: Customer) => {
    val featureValues = CustomerFeatureSet.generate(c)

    val expectTallAge   = c.height > 2.0
    val expectOldHieght = c.age > 65

    featureValues must_== List(
                        Some(FeatureValue[Integral](c.id, "age",       c.age)),
      expectTallAge.option(  FeatureValue[Integral](c.id, "tallAge",   c.age)),
      expectOldHieght.option(FeatureValue[Decimal] (c.id, "oldHeight", c.height))
    ).flatten
  }}
}

object AggregationFeatureSetSpec extends Specification with ScalaCheck { def is = s2"""
  AggregationFeatureSet - Test an example set of features based on aggregating records
  ===========
  An example feature set
    must generate expected metadata       $generateMetadata
    must generate expected feature values $generateFeatureValues
"""

  import Type._

  object CustomerFeatureSet extends AggregationFeatureSet[Customer] {
    val namespace           = "test.namespace"
    def entity(c: Customer) = c.id
    def time(c: Customer, ctx: FeatureContext)   = ctx.generationTime.getMillis

    val source = From[Customer]()
    val select = source.featureSetBuilder(namespace, entity)

    type CAF = AggregationFeature[Customer, _, Value]

    val sizeF:  CAF = select(size )                     .asFeature(Discrete,    "size",  "Agg feature")
    val countF: CAF = select(count(where = _.age >= 18)).asFeature(Continuous,  "count", "Agg feature")
    val sumF:   CAF = select(sum(_.height))             .asFeature(Continuous,  "sum",   "Agg feature")
    val maxF:   CAF = select(max(_.age))                .asFeature(Continuous,  "max",   "Agg feature")
    val minF:   CAF = select(min(_.height))             .asFeature(Continuous,  "min",   "Agg feature")
    val avgF:   CAF = select(avg(_.age.toDouble))       .asFeature(Continuous,  "avg",   "Agg feature")
    val cuF:    CAF = select(uniqueCountBy(_.age % 10)) .asFeature(Continuous,  "cu",    "Agg feature")

    def aggregationFeatures = List(sizeF, countF, sumF, maxF, minF, avgF, cuF)
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.metadata
    import CustomerFeatureSet.namespace

    metadata must_== List(
      Metadata[Customer, Integral](namespace, "size", "Agg feature",  Discrete),
      Metadata[Customer, Integral](namespace, "count","Agg feature",  Continuous),
      Metadata[Customer, Decimal] (namespace, "sum",  "Agg feature",  Continuous),
      Metadata[Customer, Decimal] (namespace, "max",  "Agg feature",  Continuous),
      Metadata[Customer, Decimal] (namespace, "min",  "Agg feature",  Continuous),
      Metadata[Customer, Decimal] (namespace, "avg",  "Agg feature",  Continuous),
      Metadata[Customer, Integral](namespace, "cu",   "Agg feature",  Continuous)
    )
  }

  def generateFeatureValues = forAll { (cs: NonEmptyList[Customer]) => {
    val featureValues = CustomerFeatureSet.generate((cs.head.id, cs.list))

    val eavtValues = featureValues.map { fv => fv.asEavt(0) }.toList

    val c = cs.head
    val heights = cs.map(_.height).list
    val ages = cs.map(_.age).list
    val groupedAges = cs.map(_.age).list.groupBy(_ % 10)

    eavtValues must matchEavts(List(
      (c.id, "size",  cs.size:                         Integral, 0L),
      (c.id, "count", ages.filter(_ >= 18).size:       Integral, 0L),
      (c.id, "sum",   heights.sum:                     Decimal,  0L),
      (c.id, "max",   ages.max:                        Integral, 0L),
      (c.id, "min",   heights.min:                     Decimal,  0L),
      (c.id, "avg",   (ages.sum / ages.size.toDouble): Decimal,  0L),
      (c.id, "cu",    groupedAges.size:                Integral, 0L)
    ))
  }}

  def matchEavts(expected: List[(EntityId, Name, Value, Time)])
      : Matcher[List[(EntityId, Name, Value, Time)]] =
    expected.contain(_.zip(===, ===, matchValue, ===))

  def matchValue(expected: Value): Matcher[Value] = expected match {
    case Integral(_) | Str(_) | Decimal(None) => be_===(expected)
    case Decimal(Some(expectedDouble)) =>
      beCloseTo(expectedDouble +/- 0.000000000001) ^^ (
        (v: Value) => v match {
          case Decimal(Some(actualDouble)) => actualDouble
          case _ => Double.NaN
        }
      )
  }
}
