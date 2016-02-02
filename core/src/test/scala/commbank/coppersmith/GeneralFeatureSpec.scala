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
    must pass description through  $metadataDescription
    must pass feature type through $metadataFeatureType

  Generating general feature values
    must use specified id as entity      $valueEntity
    must use specified name as name      $valueName
    must use value as defined            $valueValue
"""

  def general(
    field:     Field[Customer, _],
    filter:    Boolean,
    namespace: Namespace             = "",
    name:      Name                  = "",
    desc:      Description           = "",
    fType:     Type                  = Nominal,
    entity:    Customer => EntityId  = _.id
  ) = {
    val feature: (Namespace, Name, Description, Type, Customer => EntityId) => Feature[Customer, Value] =
      field match {
        case f if f == Fields[Customer].Name   => general[Str](fValue = c => filter.option(c.name))
        case f if f == Fields[Customer].Age    => general[Integral](fValue = c => filter.option(c.age))
        case f if f == Fields[Customer].Height => general[Decimal](fValue = c => filter.option(c.height))
      }
    feature.apply(namespace, name, desc, fType, entity)
  }

  def general[V <: Value : TypeTag](
    fValue: Customer => Option[V]
  )(
    namespace: Namespace,
    name:      Name,
    desc:      Description,
    fType:     Type,
    entity:    Customer => EntityId
  ) = Patterns.general[Customer, V, V](namespace, name, desc, fType, entity, fValue)

  def metadataNamespace = forAll { (field: Field[Customer, _], filter: Boolean, namespace: Namespace) => {
    val feature = general(field, filter, namespace = namespace)
    feature.metadata.namespace must_== namespace
  }}

  def metadataName = forAll { (field: Field[Customer, _], filter: Boolean, name: Name) => {
    val feature = general(field, filter, name = name)
    feature.metadata.name must_== name
  }}

  def metadataDescription = forAll { (field: Field[Customer, _], filter: Boolean, desc: Description) => {
    val feature = general(field, filter, desc = desc)
    feature.metadata.description must_== desc
  }}

  def metadataFeatureType = forAll { (field: Field[Customer, _], filter: Boolean, fType: Type) => {
    val feature = general(field, filter, fType = fType)
    feature.metadata.featureType must_== fType
  }}

  def valueEntity = forAll { (field: Field[Customer, _], c: Customer) => {
    val feature = general(field, true, entity = _.id)
    feature.generate(c) must beSome.like { case v => v.entity must_== c.id }
  }}

  def valueName = forAll {
    (field: Field[Customer, _], ns: Namespace, name: String, fType: Type, c: Customer) => {
      val feature = general(field, true, name = name)
      feature.generate(c) must beSome.like { case v => v.name must_== name }
    }
  }

  def valueValue = forAll { (field: Field[Customer, _], filter: Boolean, c: Customer) => {
    val feature = general(field, filter)

    val expectedValue = field match {
      case f if f == Fields[Customer].Name   => Str(Option(c.name))
      case f if f == Fields[Customer].Age    => Integral(Option(c.age))
      case f if f == Fields[Customer].Height => Decimal(Option(c.height))
    }

    val featureValue = feature.generate(c)
    if (!filter) featureValue must beNone
    else featureValue must beSome.like { case v => v.value must_== expectedValue }
  }}
}
