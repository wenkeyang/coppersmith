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

import org.joda.time.Interval

import Feature._, Value._, Type._

import Arbitraries._

import test.thrift.Customer


object GeneralFeatureSpec extends Specification with ScalaCheck { def is = s2"""
  General Features - Test individual feature components
  ===========
  Creating general feature metadata
    must pass namespace through        $metadataNamespace
    must pass name through             $metadataName
    must pass description through      $metadataDescription
    must pass feature type through     $metadataFeatureType
    must pass range through            $metadataRange
    must pass validityInterval through $validityInterval

  Generating general feature values
    must use specified id as entity $valueEntity
    must use specified name as name $valueName
    must use value as defined       $valueValue
"""

  def general(
    rfp:       RangeFieldPair,
    filter:    Boolean,
    namespace: Namespace            = "",
    name:      Name                 = "",
    desc:      Description          = "",
    fType:     Type                 = Nominal,
    entity:    Customer => EntityId = _.id,
    validity:  Option[Interval]     = None
  ) = {
    val feature: (Namespace, Name, Description, Type, Customer => EntityId, Option[Interval]) => Feature[Customer, Value] =
      rfp match {
        case StrRangeFieldPair(r, _) => general[Str](
          fValue = c => filter.option(c.name),
          range  = r
        )
        case IntegralRangeFieldPair(r, _) => general[Integral](
          fValue = c => filter.option(c.age),
          range  = r
        )
        case FloatingPointRangeFieldPair(r, _) => general[FloatingPoint](
          fValue = c => filter.option(c.height),
          range  = r
        )
      }
    feature.apply(namespace, name, desc, fType, entity, validity)
  }

  def general[V <: Value : TypeTag](
    fValue: Customer => Option[V],
    range:  Option[Feature.Value.Range[V]]
  )(
    namespace: Namespace,
    name:      Name,
    desc:      Description,
    fType:     Type,
    entity:    Customer => EntityId,
    validity:  Option[Interval]
  ) = Patterns.general[Customer, V](namespace, name, desc, fType, entity, fValue, range, validity)

  def metadataNamespace = forAll { (rfp: RangeFieldPair, filter: Boolean, namespace: Namespace) => {
    val feature = general(rfp, filter, namespace = namespace)
    feature.metadata.namespace must_== namespace
  }}

  def metadataName = forAll { (rfp: RangeFieldPair, filter: Boolean, name: Name) => {
    val feature = general(rfp, filter, name = name)
    feature.metadata.name must_== name
  }}

  def metadataDescription = forAll { (rfp: RangeFieldPair, filter: Boolean, desc: Description) => {
    val feature = general(rfp, filter, desc = desc)
    feature.metadata.description must_== desc
  }}

  def metadataFeatureType = forAll { (rfp: RangeFieldPair, filter: Boolean, fType: Type) => {
    val feature = general(rfp, filter, fType = fType)
    feature.metadata.featureType must_== fType
  }}

  def metadataRange = forAll { (rfp: RangeFieldPair, filter: Boolean) => {
    val feature = general(rfp, filter)
    feature.metadata.valueRange must_== rfp.range
  }}

  def validityInterval = forAll { (rfp: RangeFieldPair, filter: Boolean, validityInterval: Option[Interval]) => {
    val feature = general(rfp, filter, validity = validityInterval)
    feature.metadata.validityInterval must_== validityInterval
  }}

  def valueEntity = forAll { (rfp: RangeFieldPair, c: Customer) => {
    val feature = general(rfp, true, entity = _.id)
    feature.generate(c) must beSome.like { case v => v.entity must_== c.id }
  }}

  def valueName = forAll {
    (rfp: RangeFieldPair, ns: Namespace, name: String, fType: Type, c: Customer) => {
      val feature = general(rfp, true, name = name)
      feature.generate(c) must beSome.like { case v => v.name must_== name }
    }
  }

  def valueValue = forAll { (rfp: RangeFieldPair, filter: Boolean, c: Customer) => {
    val feature = general(rfp, filter)

    val expectedValue = rfp match {
      case _: StrRangeFieldPair           => Str(Option(c.name))
      case _: IntegralRangeFieldPair      => Integral(Option(c.age))
      case _: FloatingPointRangeFieldPair => FloatingPoint(Option(c.height))
    }

    val featureValue = feature.generate(c)
    if (!filter) featureValue must beNone
    else featureValue must beSome.like { case v => v.value must_== expectedValue }
  }}
}
