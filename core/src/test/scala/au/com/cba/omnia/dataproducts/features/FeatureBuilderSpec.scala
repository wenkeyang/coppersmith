package au.com.cba.omnia.dataproducts.features

import scalaz.NonEmptyList
import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean
import scalaz.syntax.std.option.ToOptionIdOps

import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary

import org.scalacheck.Prop.forAll

import org.specs2._, matcher.Matcher

import Feature._, Value._, Type.{Categorical, Continuous}
import FeatureSetBuilder.FeatureSetBuilderSource

import Arbitraries._

import au.com.cba.omnia.dataproducts.features.test.thrift.Customer

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
    def time(c: Customer)   = c.time

    val source = From[Customer]()
    val select = source.featureSetBuilder(namespace, entity(_), time(_))

    type CustFeature = Feature[Customer, Value]
    val age:       CustFeature = select(_.age)                      .asFeature("age", "Age",              Categorical)
    val tallAge:   CustFeature = select(_.age).where(_.height > 2.0).asFeature("tallAge", "Tall Age",     Continuous)
    val oldHeight: CustFeature = select(_.height).where(_.age > 65) .asFeature("oldHeight", "Old Height", Continuous)

    def features = List(age, tallAge, oldHeight)
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.generateMetadata

    metadata must_== List(
      FeatureMetadata[Integral](CustomerFeatureSet.namespace, "age", "Age",      Categorical),
      FeatureMetadata[Integral](CustomerFeatureSet.namespace, "tallAge", "Tall Age", Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "oldHeight", "Old Height", Continuous)
    )
  }

  def generateFeatureValues = forAll { (c: Customer) => {
    val featureValues = CustomerFeatureSet.generate(c)

    val expectTallAge   = c.height > 2.0
    val expectOldHieght = c.age > 65

    featureValues must_== List(
                        Some(FeatureValue[Integral](c.id, "age",       c.age,    c.time)),
      expectTallAge.option(  FeatureValue[Integral](c.id, "tallAge",   c.age,    c.time)),
      expectOldHieght.option(FeatureValue[Decimal] (c.id, "oldHeight", c.height, c.time))
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

  import Type.{Categorical, Continuous}

  object CustomerFeatureSet extends AggregationFeatureSet[Customer] {
    val namespace           = "test.namespace"
    def entity(c: Customer) = c.id
    def time(c: Customer)   = c.time

    val source = From[Customer]()
    val select = source.featureSetBuilder(namespace, entity(_), time(_))

    type CustAggFeature = AggregationFeature[Customer, _, Value]

    val sizeF:  CustAggFeature = select(size )                     .asFeature("size", "Agg feature",  Categorical)
    val countF: CustAggFeature = select(count(where = _.age >= 18)).asFeature("count", "Agg feature", Continuous)
    val sumF:   CustAggFeature = select(sum(_.height))             .asFeature("sum", "Agg feature",   Continuous)
    val maxF:   CustAggFeature = select(max(_.age))                .asFeature("max", "Agg feature",   Continuous)
    val minF:   CustAggFeature = select(min(_.height))             .asFeature("min", "Agg feature",   Continuous)
    val avgF:   CustAggFeature = select(avg(_.age.toDouble))       .asFeature("avg", "Agg feature",   Continuous)

    def aggregationFeatures = List(sizeF, countF, sumF, maxF, minF, avgF)
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.generateMetadata

    metadata must_== List(
      FeatureMetadata[Integral](CustomerFeatureSet.namespace, "size", "Agg feature",  Categorical),
      FeatureMetadata[Integral](CustomerFeatureSet.namespace, "count","Agg feature",  Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "sum",  "Agg feature",  Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "max",  "Agg feature",  Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "min",  "Agg feature",  Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "avg",  "Agg feature",  Continuous)
    )
  }

  def generateFeatureValues = forAll { (cs: NonEmptyList[Customer]) => {
    val featureValues = CustomerFeatureSet.generate((cs.head.id, cs.list)).map(_.asEavt).toList

    val c = cs.head
    val heights = cs.map(_.height).list
    val ages = cs.map(_.age).list

    featureValues must matchEavts(List(
      (c.id, "size",  cs.size:                         Integral, c.time),
      (c.id, "count", ages.filter(_ >= 18).size:       Integral, c.time),
      (c.id, "sum",   heights.sum:                     Decimal,  c.time),
      (c.id, "max",   ages.max:                        Integral, c.time),
      (c.id, "min",   heights.min:                     Decimal,  c.time),
      (c.id, "avg",   (ages.sum / ages.size.toDouble): Decimal,  c.time)
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
