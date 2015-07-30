package au.com.cba.omnia.dataproducts.features

import scalaz.NonEmptyList
import scalaz.syntax.std.option.ToOptionIdOps

import scalaz.scalacheck.ScalazArbitrary.NonEmptyListArbitrary

import org.scalacheck.Prop.forAll

import org.specs2._, matcher.Matcher

import Feature._, Value._

import Arbitraries._

import au.com.cba.omnia.dataproducts.features.test.thrift.Customer

object AggregationFeatureSetSpec extends Specification with ScalaCheck { def is = s2"""
  AggregationFeatureSet - Test an example set of features based on aggregating records
  ===========
  An example feature set
    must generate expected metadata       $generateMetadata
    must generate expected feature values $generateFeatureValues
"""

  import Type.Continuous

  object CustomerFeatureSet extends AggregationFeatureSet[Customer] {
    val namespace   = "test.namespace"

    def entity(c: Customer) = c.id
    def time(c: Customer)   = c.time

    import AggregationFeature.FeatureBuilder

    type CustAggFeature = AggregationFeature[Customer, _, Value]
    val sizeF:  CustAggFeature = size                      .asFeature("size")
    val countF: CustAggFeature = count(where = _.age >= 18).asFeature("count")
    val sumF:   CustAggFeature = sum(_.height)             .asFeature("sum")
    val maxF:   CustAggFeature = max(_.age)                .asFeature("max")
    val minF:   CustAggFeature = min(_.height)             .asFeature("min")
    val avgF:   CustAggFeature = avg(_.age.toDouble)       .asFeature("avg")

    def aggregationFeatures = List(sizeF, countF, sumF, maxF, minF, avgF)
  }

  def generateMetadata = {
    val metadata = CustomerFeatureSet.generateMetadata

    metadata must_== List(
      FeatureMetadata[Integral](CustomerFeatureSet.namespace, "size",  Continuous),
      FeatureMetadata[Integral](CustomerFeatureSet.namespace, "count", Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "sum",   Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "max",   Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "min",   Continuous),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, "avg",   Continuous)
    )
  }

  def generateFeatureValues = forAll { (cs: NonEmptyList[Customer]) => {
    val featureValues = CustomerFeatureSet.generate((cs.head.id, cs.list)).map(_.toEavt).toList

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
