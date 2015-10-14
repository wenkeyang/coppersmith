package commbank.coppersmith

import scalaz.{Value => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

import org.scalacheck.{Arbitrary, Gen}, Arbitrary._, Gen._

import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api.{Field, Maestro}, Maestro.Fields

import Feature.{Value, Type}, Value._, Type._

import test.thrift.{Account, Customer}

object Arbitraries {
  implicit val arbFeatureType: Arbitrary[Type] = Arbitrary(oneOf(Nominal, Continuous, Ordinal, Discrete))

  implicit val integralValueGen: Gen[Integral] = arbitrary[Option[Long]].map(Integral(_))
  implicit val decimalValueGen: Gen[Decimal] = arbitrary[Option[Double]].map(Decimal(_))
  implicit val strValueGen: Gen[Str] = arbitrary[Option[String]].map(Str(_))
  implicit val arbValue: Arbitrary[Value] = Arbitrary(oneOf(integralValueGen, decimalValueGen, strValueGen))

  val arbTime: Gen[Long] =
      for { year <- chooseNum(1970, 2100); month <- chooseNum(1, 12); day <- chooseNum(1, 28) }
      yield new DateTime(year, month, day, 0, 0).getMillis

  implicit val arbFeatureValue: Arbitrary[FeatureValue[Value]] = Arbitrary(
    (arbNonEmptyAlphaStr.map(_.value) |@|
       arbNonEmptyAlphaStr.map(_.value) |@|
       arbitrary[Value] |@|
       arbTime
    )(FeatureValue.apply _)
  )

  def ageGen: Gen[Int] = Gen.chooseNum(0, 150, 17, 18, 19, 64, 65, 66)
  implicit val arbCustomer: Arbitrary[Customer] = Arbitrary(
    (arbitrary[String] |@|
       arbitrary[String] |@|
       ageGen |@|
       arbitrary[Double] |@|
       arbitrary[Option[Double]] |@|
       arbitrary[Long])(Customer.apply)
  )

  implicit val arbCustomerField: Arbitrary[Field[Customer, _]] = Arbitrary(
    oneOf(Fields[Customer].Name, Fields[Customer].Age, Fields[Customer].Height)
  )

  implicit val arbAccount: Arbitrary[Account] = Arbitrary(
    (arbitrary[String] |@|
       arbitrary[String] |@|
       arbitrary[Double] |@|
       arbitrary[Option[String]] |@|
       arbitrary[Option[Int]] |@|
       arbitrary[Option[Double]] |@|
       arbTime)(Account.apply)
  )

  case class CustomerAccountsGroup(c: Customer, as: Iterable[Account])
  case class CustomerAccounts(cas: Iterable[CustomerAccountsGroup])
  implicit val arbCustomerAccounts: Arbitrary[CustomerAccounts] = {
    implicit val arbCA: Arbitrary[CustomerAccountsGroup] = Arbitrary(
      for {
        c <- arbitrary[Customer]
        as <- arbitrary[Iterable[Account]]
      } yield CustomerAccountsGroup(c, as.map(_.copy(customerId = c.id)))
    )
    Arbitrary(
      arbitrary[Iterable[CustomerAccountsGroup]].map(cas => {
        // Filter out customers with the same id (typically from multiple empty strings being generated)
        val uniqueByCustomerId = cas.groupBy(_.c.id).values.flatMap(_.headOption.toList)
        CustomerAccounts(uniqueByCustomerId)
      })
    )
  }

  // Not specific to coppersmith - consider moving to project like omnitool
  final case class NonEmptyString private(head: Char, tail: String) { val value = head +: tail }
  object NonEmptyString {
    def nonEmptyString(h: Char, t: String): NonEmptyString = NonEmptyString(h, t)
  }
  import NonEmptyString.nonEmptyString
  implicit val arbNonEmptyStr: Arbitrary[NonEmptyString] =
    Arbitrary(for (h <- arbitrary[Char]; t <- arbitrary[String]) yield nonEmptyString(h, t))

  val arbNonEmptyAlphaStr: Gen[NonEmptyString] =
    for (h <- alphaChar; t <- alphaStr) yield nonEmptyString(h, t)

}
