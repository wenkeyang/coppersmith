package commbank.coppersmith

import scalaz.{Value => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

import org.scalacheck.{Arbitrary,Gen}, Arbitrary._, Gen._

import au.com.cba.omnia.maestro.api.{Field, Maestro}, Maestro.Fields

import Feature.{Value, Type}, Value._, Type._

import test.thrift.Customer

object Arbitraries {
  implicit val arbFeatureType: Arbitrary[Type] = Arbitrary(oneOf(Categorical, Continuous))

  implicit val integralValueGen: Gen[Integral] = arbitrary[Option[Long]].map(Integral(_))
  implicit val decimalValueGen: Gen[Decimal] = arbitrary[Option[Double]].map(Decimal(_))
  implicit val strValueGen: Gen[Str] = arbitrary[Option[String]].map(Str(_))
  implicit val arbValue: Arbitrary[Value] = Arbitrary(oneOf(integralValueGen, decimalValueGen, strValueGen))

  def ageGen: Gen[Int] = Gen.chooseNum(0, 150, 17, 18, 19, 64, 65, 66)
  implicit val arbCustomer: Arbitrary[Customer] = Arbitrary(
    (arbitrary[String] |@| arbitrary[String] |@| ageGen |@| arbitrary[Double] |@| arbitrary[Long])(Customer.apply)
  )

  implicit val arbCustomerField: Arbitrary[Field[Customer, _]] = Arbitrary(
    oneOf(Fields[Customer].Name, Fields[Customer].Age, Fields[Customer].Height)
  )

}
