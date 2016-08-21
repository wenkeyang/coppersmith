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

import java.util.concurrent.TimeUnit.MILLISECONDS

import org.joda.time.{Period, DateTimeZone, LocalDate, DateTime}
import org.joda.time.format.DateTimeFormat

import org.specs2.{ScalaCheck, Specification}

import org.scalacheck.Prop._

import commbank.coppersmith.Arbitraries._
import commbank.coppersmith.util.{DatePeriod, Timestamp, Datestamp}, Timestamp.Offset

object TimeSpec extends Specification with ScalaCheck { def is = s2"""
  Parse valid date string            $parseDate
  Not parse invalid date string      $parseInvalidDate
  Parse valid time string            $parseValidTime
  Not parse invalid time string      $parseInvalidTime
  Print valid date string            $printDate
  Print valid time string            $printTime
  Calculate correct date differences $dateDiff
  Order dates correctly              $dateOrder
  Order times correctly              ${all(timeOrder, equalMillisTimeOrder, timeZoneOrder)}
"""

  def parseDate = forAll { (dateTime: DateTime) => {
    val dateStr  = dateTime.toString("yyyy-MM-dd")
    val dateStrP = dateTime.toString("dd-MMM-yyyy")
    val expected = Datestamp(dateTime.getYear, dateTime.getMonthOfYear, dateTime.getDayOfMonth)
    Seq(
      Datestamp.parse(dateStr).right.get                       must_== expected,
      Datestamp.unsafeParse(dateStr)                           must_== expected,
      Datestamp.parseFormat("dd-MMM-yyyy")(dateStrP).right.get must_== expected,
      Datestamp.unsafeParseFormat("dd-MMM-yyyy")(dateStrP)     must_== expected
    )
  }}

  def parseInvalidDate = forAll { (dateTime: DateTime) => {
    val dateStr = dateTime.toString("dd-MM-yyyy")
    val pattern = "yyyy-MM-dd"
    val expected = (dateStr, pattern)

    Seq(
      Datestamp.parse(dateStr).left.get                must_== expected,
      Datestamp.parseFormat(pattern)(dateStr).left.get must_== expected,
      Datestamp.unsafeParse(dateStr)
        must throwA(new RuntimeException(s"Unable to parse date: $expected")),
      Datestamp.unsafeParseFormat(pattern)(dateStr)
        must throwA(new RuntimeException(s"Unable to parse date: $expected"))

    )
  }}

  def parseValidTime = forAll { (dateTime: DateTime) => {
    def testParse(dt: DateTime, format: String, p: (String) => Timestamp, e: (DateTime) => Timestamp) = {
      val s = dt.toString(format)
      val t = p(s)
      t must_== e(dt)
    }
    def parseOffset(dt: DateTime) = {
      val offsetL = dt.getZone.getOffset(dateTime)
      Some((MILLISECONDS.toHours(offsetL).toInt,
        Math.abs(MILLISECONDS.toMinutes(offsetL).toInt % 60)))
    }
    def toUTC(dt: DateTime): DateTime = {
      dt.withZoneRetainFields(DateTimeZone.UTC)
    }
    Seq(
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
        Timestamp.parseWithMillis(_).right.get,
        dt => Timestamp(dt.getMillis, parseOffset(dt))
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ssZZ",
        Timestamp.parseWithoutMillis(_).right.get,
        dt => Timestamp(dt.withMillisOfSecond(0).getMillis, parseOffset(dt))
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ss.SSS'-00:00'",
        Timestamp.parseWithMillis(_).right.get,
        dt => Timestamp(toUTC(dt).getMillis, None)
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ss'-00:00'",
        Timestamp.parseWithoutMillis(_).right.get,
        dt => Timestamp(toUTC(dt).withMillisOfSecond(0).getMillis, None)
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
        Timestamp.unsafeParseWithMillis,
        dt => Timestamp(dt.getMillis, parseOffset(dt))
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ssZZ",
        Timestamp.unsafeParseWithoutMillis,
        dt => Timestamp(dt.withMillisOfSecond(0).getMillis, parseOffset(dt))
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ss.SSS'-00:00'",
        Timestamp.unsafeParseWithMillis,
        dt => Timestamp(toUTC(dt).getMillis, None)
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ss'-00:00'",
        Timestamp.unsafeParseWithoutMillis,
        dt => Timestamp(toUTC(dt).withMillisOfSecond(0).getMillis, None)
      ),
      testParse(dateTime,
        "yyyy-dd-MM'T'HH:mm:ss.SSSZZ",
        Timestamp.parseFormat("yyyy-dd-MM'T'HH:mm:ss.SSSZZ")(_).right.get,
        dt => Timestamp(dt.getMillis, parseOffset(dt))
      ),
      testParse(dateTime,
        "yyyy-dd-MM'T'HH:mm:ss.SSSZZ",
        Timestamp.parseFormatWithOffset("yyyy-dd-MM'T'HH:mm:ss.SSSZZ")(_, None).right.get,
        dt => Timestamp(toUTC(dt).getMillis, None)
     ),
      testParse(dateTime,
        "yyyy-dd-MM'T'HH:mm:ss.SSSZZ",
        Timestamp.unsafeParseFormat("yyyy-dd-MM'T'HH:mm:ss.SSSZZ")(_),
        dt => Timestamp(dt.getMillis, parseOffset(dt))
      ),
      testParse(dateTime,
        "yyyy-dd-MM'T'HH:mm:ss.SSSZZ",
        Timestamp.unsafeParseFormatWithOffset("yyyy-dd-MM'T'HH:mm:ss.SSSZZ")(_, None),
        dt => Timestamp(toUTC(dt).getMillis, None)
      )
    )
  }}

  def parseInvalidTime = forAll { (dateTime: DateTime) => {
    val invalidTimeStr = dateTime.toString("dd-MM-yyyy'T'HH:mm:ss.SSSZZ")
    val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    val noTzPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    val expected = (invalidTimeStr, pattern)
    val noMillisExpected = (invalidTimeStr, "yyyy-MM-dd'T'HH:mm:ssZZ")

    Seq(
      Timestamp.parseWithMillis(invalidTimeStr).left.get                      must_== expected,
      Timestamp.parseWithoutMillis(invalidTimeStr).left.get                   must_== noMillisExpected,
      Timestamp.parseFormat(pattern)(invalidTimeStr).left.get                 must_== expected,
      Timestamp.parseFormatWithOffset(pattern)(invalidTimeStr, None).left.get must_== expected,
      Timestamp.unsafeParseWithMillis(invalidTimeStr)
        must throwA(new RuntimeException(s"Unable to parse time: $expected")),
      Timestamp.unsafeParseWithoutMillis(invalidTimeStr)
        must throwA(new RuntimeException(s"Unable to parse time: $noMillisExpected")),
      Timestamp.unsafeParseFormat(pattern)(invalidTimeStr)
        must throwA(new RuntimeException(s"Unable to parse time: $expected")),
      Timestamp.unsafeParseFormatWithOffset(pattern)(invalidTimeStr, None)
        must throwA(new RuntimeException(s"Unable to parse time: $expected")),

      Timestamp.parseFormat(noTzPattern)
        must throwA(new IllegalArgumentException(s"$noTzPattern doesn't parse timezones."))

    )
  }}

  def printDate = forAll { (date: Datestamp) => {
    val dateStr  = date.toIso8601ExtendedFormatString
    val dateTime = DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate(dateStr)

    dateTime must_== new LocalDate(date.year, date.month, date.day)
  }}

  def printTime = forAll { (time: Timestamp) => {
    val timeStr  = time.toRfc3339String
    val dateTime = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
      .withOffsetParsed().parseDateTime(timeStr)
    val (h, m)   = time.offset.getOrElse((0,0))
    val zone     = DateTimeZone.forOffsetHoursMinutes(h, m)
    val matchers = Seq(dateTime must_== new DateTime(time.millis, zone))
    if (time.offset.isEmpty) matchers :+ (timeStr must endWith("-00:00"))
    else matchers
  }}

  def dateDiff = forAll { (d1: Datestamp, d2: Datestamp) =>
    val ld1 = new LocalDate(d1.year, d1.month, d1.day)
    val ld2 = new LocalDate(d2.year, d2.month, d2.day)

    val dp = d1.difference(d2)
    val p = new Period(ld1, ld2)

    dp must_== DatePeriod(p.getYears, p.getMonths, p.getDays)
  }

  def dateOrder = forAll { (d1: Datestamp, d2: Datestamp) => {
    val ld1 = new LocalDate(d1.year, d1.month, d1.day)
    val ld2 = new LocalDate(d2.year, d2.month, d2.day)
    ld1.compareTo(ld2) must_== implicitly[Ordering[Datestamp]].compare(d1, d2)
  }}

  def timeOrder = forAll { (t1: Timestamp, t2: Timestamp) => {
    (t1, t2) match {
      case (Timestamp(m1, o1), Timestamp(m2, o2)) if m1 > m2 => t1 must beGreaterThan(t2)
      case (Timestamp(m1, o1), Timestamp(m2, o2)) if m1 == m2 =>
        implicitly[Ordering[Timestamp]].compare(t1, t2) must_== implicitly[Ordering[Offset]].compare(o1, o2)
      case (Timestamp(m1, o1), Timestamp(m2, o2)) if m1 < m2 => t1 must beLessThan(t2)
    }
  }}

  def equalMillisTimeOrder = forAll { (t: Timestamp, o: Offset) => {
    implicitly[Ordering[Timestamp]].compare(t, t.copy(offset = o)) must_==
      implicitly[Ordering[Offset]].compare(t.offset, o)
  }}

  def timeZoneOrder = {
    val utc = Timestamp.unsafeParseWithMillis("2000-06-06T10:00:00.000+00:00")
    val aest = Timestamp.unsafeParseWithMillis("2000-06-06T20:00:00.000+10:00")
    val unknownTimezone = Timestamp.unsafeParseWithMillis("2000-06-06T20:00:00.000-00:00")
    Seq(
      utc must beLessThan(aest),
      utc must beLessThan(unknownTimezone)
    )
  }
}
