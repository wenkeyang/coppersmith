package commbank.coppersmith.scalding

import au.com.cba.omnia.maestro.api.Maestro.Fields
import au.com.cba.omnia.maestro.api.{Field, Maestro}
import commbank.coppersmith.Feature.Type._
import commbank.coppersmith.Feature.Value._
import commbank.coppersmith.Feature.{Type, Value}
import commbank.coppersmith._
import commbank.coppersmith.test.thrift.Customer
import org.joda.time.DateTime
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import org.scalacheck.{Arbitrary, Gen}

import scalaz.Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

object ScaldingArbitraries {
  implicit val arbFeatureType: Arbitrary[Type] = Arbitrary(oneOf(Categorical, Continuous))

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
    (arbitrary[String] |@| arbitrary[String] |@| ageGen |@| arbitrary[Double] |@| arbitrary[Long])(Customer.apply)
  )

  implicit val arbCustomerField: Arbitrary[Field[Customer, _]] = Arbitrary(
    oneOf(Fields[Customer].Name, Fields[Customer].Age, Fields[Customer].Height)
  )

  // Not specific to coppersmith - consider moving to project like omnitool
  import org.apache.hadoop.fs.Path
  final case class NonEmptyString private(head: Char, tail: String) { val value = head +: tail }
  object NonEmptyString {
    def nonEmptyString(h: Char, t: String): NonEmptyString = NonEmptyString(h, t)
  }
  import NonEmptyString.nonEmptyString
  implicit val arbNonEmptyStr: Arbitrary[NonEmptyString] =
    Arbitrary(for (h <- arbitrary[Char]; t <- arbitrary[String]) yield nonEmptyString(h, t))

  val arbNonEmptyAlphaStr: Gen[NonEmptyString] =
    for (h <- alphaChar; t <- alphaStr) yield nonEmptyString(h, t)

  implicit val arbPath: Arbitrary[Path] = Arbitrary(arbNonEmptyAlphaStr.map(nes => new Path(nes.value)))
}
