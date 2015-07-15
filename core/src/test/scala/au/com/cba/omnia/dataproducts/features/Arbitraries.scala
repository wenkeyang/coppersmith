package au.com.cba.omnia.dataproducts.features

import scala.reflect.runtime.universe.TypeTag

import org.scalacheck.{Arbitrary,Gen}, Arbitrary._, Gen._

import au.com.cba.omnia.maestro.api.{Field, Maestro}, Maestro.Fields

import Feature._, Value._, Type._

import au.com.cba.omnia.dataproducts.features.test.thrift._

object Arbitraries {
  implicit val arbFeatureType: Arbitrary[Type] = Arbitrary(oneOf(Categorical, Continuous))

  implicit val integralValueGen: Gen[Integral] = arbitrary[Option[Long]].map(Integral(_))
  implicit val decimalValueGen: Gen[Decimal] = arbitrary[Option[Double]].map(Decimal(_))
  implicit val strValueGen: Gen[Str] = arbitrary[Option[String]].map(Str(_))
  implicit val arbValue: Arbitrary[Value] = Arbitrary(oneOf(integralValueGen, decimalValueGen, strValueGen))

  implicit val arbCustomer: Arbitrary[Customer] = Arbitrary(
    Gen.resultOf[String, String, Int, Double, Long, Customer](Customer.apply)
  )

  implicit val arbCustomerField: Arbitrary[Field[Customer, _]] = Arbitrary(
    oneOf(Fields[Customer].Name, Fields[Customer].Age, Fields[Customer].Height)
  )

}
