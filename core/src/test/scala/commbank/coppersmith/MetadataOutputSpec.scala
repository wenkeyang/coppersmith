package commbank.coppersmith

import commbank.coppersmith.Feature.Type.{Categorical, Numeric}
import commbank.coppersmith.Feature.Value.{Str, Decimal, Integral}
import commbank.coppersmith.Feature._
import commbank.coppersmith.test.thrift.Customer
import org.scalacheck.Prop._
import org.specs2.{ScalaCheck, Specification}


import Arbitraries._

object MetadataOutputSpec extends Specification with ScalaCheck { def is = s2"""
  HydroPsv creates expected Hydro metadata $hydroPsv
  LuaTable creates expected Hydro metadata $luaTable
"""

  def hydroPsv = forAll { (namespace: Namespace, name: Name, desc: Description, fType: Type, value: Value) => {
    val (metadata, expectedValueType) = value match {
      case Integral(_) => (Metadata[Customer, Integral](namespace, name, desc, fType), "int")
      case Decimal(_)  => (Metadata[Customer, Decimal] (namespace, name, desc, fType), "double")
      case Str(_)      => (Metadata[Customer, Str]     (namespace, name, desc, fType), "string")
    }

    val expectedFeatureType = fType match {
      case n : Numeric    => "continuous"
      case c: Categorical => "categorical"
    }

    val hydroMetadata = MetadataOutput.HydroPsv(metadata)

    hydroMetadata must_==
      s"${namespace.toLowerCase}.${name.toLowerCase}|$expectedValueType|$expectedFeatureType"
  }}

  def luaTable = forAll { (namespace: Namespace, name: Name, desc: Description, fType: Type, value: Value) => {
    val (metadata, expectedValueType) = value match {
      case Integral(_) => (Metadata[Customer, Integral](namespace, name, desc, fType), "integral")
      case Decimal(_)  => (Metadata[Customer, Decimal] (namespace, name, desc, fType), "decimal")
      case Str(_)      => (Metadata[Customer, Str]     (namespace, name, desc, fType), "string")
    }

    val expectedFeatureType = fType.toString.toLowerCase

    val luaMetadata = MetadataOutput.LuaTable(metadata)

    luaMetadata must_==
      s"""FeatureMetadata{
         |    name = "${metadata.name}",
         |    namespace = "${metadata.namespace}",
         |    description = "${metadata.description}",
         |    source = "${metadata.sourceTag.tpe}",
         |    featureType = "${expectedFeatureType}",
         |    valueType = "${expectedValueType}"
         |}
     """.stripMargin
  }}
}