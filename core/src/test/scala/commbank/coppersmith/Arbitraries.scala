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

import scala.util.Try

import scalaz.scalacheck.ScalazArbitrary._
import scalaz.{Value => _, _}, Scalaz.{option => _, _}
import scalaz.scalacheck.ScalaCheckBinding._

import org.scalacheck.{Arbitrary, Gen}, Arbitrary._, Gen._

import org.joda.time.{DateTimeZone, LocalDate, DateTime}

import au.com.cba.omnia.maestro.api.{Field, Maestro}, Maestro.Fields

import Feature.{Value, Type}, Value._, Type._
import util.{Datestamp, Timestamp}

import test.thrift.{Account, Customer}

object Arbitraries {
  implicit val arbFeatureType: Arbitrary[Type] = Arbitrary(oneOf(Nominal, Continuous, Ordinal, Discrete))

  implicit val arbLocalDate: Arbitrary[LocalDate] = Arbitrary(for {
    year  <- chooseNum(1970, 2100)
    month <- chooseNum(1, 12)
    day   <- chooseNum(1, 28)
  } yield new LocalDate(year, month, day))

  implicit val arbDateTime: Arbitrary[DateTime] = Arbitrary(for {
    y   <- chooseNum(1970, 2100)
    m   <- chooseNum(1, 12)
    d   <- chooseNum(1, 28)
    h   <- chooseNum(0, 23)
    min <- chooseNum(0, 59)
    s   <- chooseNum(0, 59)
    ms  <- chooseNum(0, 999)
    zh  <- chooseNum(-23, 23)
    zm  <- chooseNum(0, 59)
  } yield new DateTime(y, m, d, h, min, s, ms, DateTimeZone.forOffsetHoursMinutes(zh, zm)))

  implicit val arbTimestamp: Arbitrary[Timestamp] = {
    import java.util.concurrent.TimeUnit.MILLISECONDS
    Arbitrary(for {
      dt     <- arbDateTime.arbitrary
      b      <- arbitrary[Boolean]
      offsetL = dt.getZone.getOffset(dt)
      offset  = (MILLISECONDS.toHours(offsetL).toInt, Math.abs(MILLISECONDS.toMinutes(offsetL).toInt % 60))
    } yield {
      Timestamp(
        dt.getMillis,
        if (b) Some(offset) else None
      )
    })
  }

  implicit val arbDatestamp: Arbitrary[Datestamp] = for {
    date <- arbLocalDate
  } yield Datestamp(date.getYear, date.getMonthOfYear, date.getDayOfMonth)

  implicit val integralValueGen: Gen[Integral] = arbitrary[Option[Long]] map Integral
  implicit val decimalValueGen: Gen[Decimal] =
    arbitrary[Option[BigDecimal]].retryUntil(obd =>
      Try(obd.hashCode).isSuccess
    ) map Decimal
  implicit val floatingPointValueGen: Gen[FloatingPoint] = arbitrary[Option[Double]] map FloatingPoint
  implicit val strValueGen: Gen[Str] = arbitrary[Option[String]] map Str
  implicit val boolValueGen: Gen[Bool] = arbitrary[Option[Boolean]] map Bool
  implicit val dateValueGen: Gen[Date] = arbitrary[Option[Datestamp]] map Date
  implicit val timeValueGen: Gen[Time] = arbitrary[Option[Timestamp]] map Time

  implicit val arbValue: Arbitrary[Value] = Arbitrary(oneOf(integralValueGen, decimalValueGen, floatingPointValueGen, strValueGen, boolValueGen, dateValueGen, timeValueGen))

  // Generates values of the same subtype, but arbitrarily chooses the subtype to generate
  implicit def arbValues: Arbitrary[NonEmptyList[Value]] =
    Arbitrary(
      oneOf(
        NonEmptyListArbitrary(Arbitrary(decimalValueGen)).arbitrary,
        NonEmptyListArbitrary(Arbitrary(floatingPointValueGen)).arbitrary,
        NonEmptyListArbitrary(Arbitrary(integralValueGen)).arbitrary,
        NonEmptyListArbitrary(Arbitrary(strValueGen)).arbitrary,
        NonEmptyListArbitrary(Arbitrary(boolValueGen)).arbitrary,
        NonEmptyListArbitrary(Arbitrary(dateValueGen)).arbitrary,
        NonEmptyListArbitrary(Arbitrary(timeValueGen)).arbitrary
      )
    )

  // Only for use with arbValues above - assumed values to be compared are of the same subtype
  implicit val valueOrder: Order[Value] =
    Order.order((a, b) => (a, b) match {
      case (Decimal(d1), Decimal(d2)) => d1.cmp(d2)
      case (FloatingPoint(d1), FloatingPoint(d2)) => d1.cmp(d2)
      case (Integral(i1), Integral(i2)) => i1.cmp(i2)
      case (Str(s1), Str(s2)) => s1.cmp(s2)
      case (Bool(b1), Bool(b2)) => b1 cmp b2
      case (Date(d1), Date(d2)) => d1.cmp(d2)
      case (Time(t1), Time(t2)) => t1.cmp(t2)
      case _ => sys.error("Assumption failed: Expected same value types from arbValues")
    })

  implicit val arbMinMaxRange: Arbitrary[MinMaxRange[Value]] = for {
    vs <- arbValues
  } yield MinMaxRange(vs.minimum1, vs.maximum1)

  implicit val arbSetRange: Arbitrary[SetRange[Value]] = for {
    vs <- arbValues
  } yield SetRange(vs.toList)

  implicit val arbRange: Arbitrary[Option[Range[Value]]] =
    Arbitrary(Gen.option(oneOf(arbMinMaxRange.arbitrary, arbSetRange.arbitrary)))

  val arbTimeMillis: Gen[Long] = arbitrary[DateTime].map(_.getMillis)

  implicit val arbFeatureValue: Arbitrary[FeatureValue[Value]] = Arbitrary(
    (arbNonEmptyAlphaStr.map(_.value) |@|
       arbNonEmptyAlphaStr.map(_.value) |@|
       arbitrary[Value]
    )(FeatureValue.apply _)
  )

  def ageGen: Gen[Int] = Gen.chooseNum(0, 150, 17, 18, 19, 64, 65, 66)
  def customerGen(strGen: Gen[String]): Gen[Customer] =
    (strGen |@|
       strGen |@|
       ageGen |@|
       arbitrary[Double] |@|
       arbitrary[Option[Double]] |@|
       arbTimeMillis)(Customer.apply)

  implicit val arbCustomer: Arbitrary[Customer] = Arbitrary(customerGen(arbitrary[String]))

  implicit val arbCustomerField: Arbitrary[Field[Customer, _]] = Arbitrary(
    oneOf(Fields[Customer].Name, Fields[Customer].Age, Fields[Customer].Height)
  )

  def accountGen(strGen: Gen[String]): Gen[Account] =
    (strGen |@|
       strGen |@|
       arbitrary[Double] |@|
       arbitrary[String] |@|
       option(strGen) |@|
       option(ageGen) |@|
       arbitrary[Option[Double]] |@|
       arbTimeMillis)(Account.apply)

  implicit val arbAccount: Arbitrary[Account] = Arbitrary(accountGen(arbitrary[String]))

  case class CustomerAccountsGroup(c: Customer, as: List[Account])
  case class CustomerAccounts(cas: Iterable[CustomerAccountsGroup])
  def arbCustomerAccounts(strGen: Gen[String]): Arbitrary[CustomerAccounts] = {
    val caGen: Gen[CustomerAccountsGroup] =
      for {
        c <- customerGen(strGen)
        as <- nonEmptyListOf[Account](accountGen(strGen))
      } yield CustomerAccountsGroup(c, as.map(_.copy(customerId = c.id)))
    Arbitrary(
      nonEmptyListOf[CustomerAccountsGroup](caGen).map(cas => {
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

  // Need take(127) as Hive will fail to store a string in "NAME" or "TBL_NAME"
  // that is over 127 chars long
  val hiveIdentifierGen: Gen[String] = arbNonEmptyAlphaStr.map(_.value.take(127))
}
