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
import commbank.coppersmith.util.{DatePeriod, Timestamp, Date}

object TimeSpec extends Specification with ScalaCheck { def is = s2"""
  Parse valid date string $parseDate
  Parse RFC3339 time string $parseRFC3339
  Print valid date string $printDate
  Print valid time string $printTime
  Calculate correct date differences $dateDiff
"""

  def parseDate = forAll { (dateTime: DateTime) => {
    val dateStr  = dateTime.toString("yyyy-MM-dd")
    val date     = Date.parse(dateStr)
    val dateStrP = dateTime.toString("dd-MMM-yyyy")
    val dateP    = Date.parse(dateStrP, "dd-MMM-yyyy")

    Seq(
      date  must_== Date(dateTime.getYear, dateTime.getMonthOfYear, dateTime.getDayOfMonth),
      dateP must_== date
    )
  }}

  def parseRFC3339 = forAll { (dateTime: DateTime) => {
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
        Timestamp.parseWithMillis,
        dt => Timestamp(dt.getMillis, parseOffset(dt))
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ssZZ",
        Timestamp.parseWithoutMillis,
        dt => Timestamp(dt.withMillisOfSecond(0).getMillis, parseOffset(dt))
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ss.SSS'-00:00'",
        Timestamp.parseWithMillis,
        dt => Timestamp(toUTC(dt).getMillis, None)
      ),
      testParse(dateTime,
        "yyyy-MM-dd'T'HH:mm:ss'-00:00'",
        Timestamp.parseWithoutMillis,
        dt => Timestamp(toUTC(dt).withMillisOfSecond(0).getMillis, None)
      )
    )
  }}

  def printDate = forAll { (date: Date) => {
    val dateStr  = date.toString
    val dateTime = DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate(dateStr)

    dateTime must_== new LocalDate(date.year, date.month, date.day)
  }}

  def printTime = forAll { (time: Timestamp) => {
    val timeStr  = time.toString
    val dateTime = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
      .withOffsetParsed().parseDateTime(timeStr)
    val (h, m)   = time.offset.getOrElse((0,0))
    val zone     = DateTimeZone.forOffsetHoursMinutes(h, m)
    val matchers = Seq(dateTime must_== new DateTime(time.millis, zone))
    if (time.offset.isEmpty) matchers :+ (timeStr must endWith("-00:00"))
    else matchers
  }}

  def dateDiff = forAll { (d1: Date, d2: Date) =>
    val ld1 = new LocalDate(d1.year, d1.month, d1.day)
    val ld2 = new LocalDate(d2.year, d2.month, d2.day)

    val dp = d1.difference(d2)
    val p = new Period(ld1, ld2)

    dp must_== DatePeriod(p.getYears, p.getMonths, p.getDays)
  }
}
