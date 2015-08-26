package commbank.coppersmith

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean

import org.scalacheck.Prop.forAll

import org.specs2._

import au.com.cba.omnia.maestro.api.{Field, Maestro}, Maestro.Fields

import Feature._, Value._, Type._

import Arbitraries._

import test.thrift.Customer


object GeneralFeatureSpec extends Specification with ScalaCheck { def is = s2"""
  General Features - Test individual feature components
  ===========
  Creating general feature metadata
    must pass namespace through    $metadataNamespace
    must pass name through         $metadataName
    must pass feature type through $metadataFeatureType

  Generating general feature values
    must use specified id as entity      $valueEntity
    must use specified name as name      $valueName
    must use value as defined            $valueValue
    must use specified time as time      $valueTime
"""

  def general[V <: Value : TypeTag](
    namespace: Namespace             = "",
    name:      Name                  = "",
    fType:     Type                  = Categorical,
    entity:    Customer => EntityId  = _.id,
    fValue:    Customer => Option[V] = (_: Customer) => Some(null),
    time:      Customer => Time      = _.time
  ) = Patterns.general(namespace, name, "Feature description", fType, entity, fValue, time)

  def metadataNamespace = forAll { (namespace: Namespace) => {
    val feature = general(namespace = namespace)
    feature.metadata.namespace must_== namespace
  }}

  def metadataName = forAll { (name: Name) => {
    val feature = general(name = name)
    feature.metadata.name must_== name
  }}

  def metadataFeatureType = forAll { (fType: Type) => {
    val feature = general(fType = fType)
    feature.metadata.featureType must_== fType
  }}

  def valueEntity = forAll { (c: Customer) => {
    val feature = general(entity = _.id)
    feature.generate(c) must beSome.like { case v => v.entity must_== c.id }
  }}

  def valueName = forAll { (namespace: Namespace, name: String, fType: Type, c: Customer) => {
    val feature = general(namespace, name, fType)
    feature.generate(c) must beSome.like { case v => v.name must_== name }
  }}

  def valueValue = forAll { (c: Customer, field: Field[Customer, _], filter: Boolean) => {
    val feature = field match {
      case f if f == Fields[Customer].Name   => general[Str](fValue = c => filter.option(c.name))
      case f if f == Fields[Customer].Age    => general[Integral](fValue = c => filter.option(c.age))
      case f if f == Fields[Customer].Height => general[Decimal](fValue = c => filter.option(c.height))
    }

    val expected = field match {
      case f if f == Fields[Customer].Name   => Str(Option(c.name))
      case f if f == Fields[Customer].Age    => Integral(Option(c.age))
      case f if f == Fields[Customer].Height => Decimal(Option(c.height))
    }

    val featureValue = feature.generate(c)
    if (!filter) featureValue must beNone else featureValue must beSome.like { case v => v.value must_== expected }
  }}

  def valueTime = forAll { (c: Customer) => {
    val feature = general(time = _.time)
    feature.generate(c) must beSome.like { case v => v.time must_== c.time }
  }}
}
