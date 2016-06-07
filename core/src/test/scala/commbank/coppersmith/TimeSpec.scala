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

import org.joda.time.{DateTimeZone, LocalDate, DateTime}
import org.joda.time.format.DateTimeFormat

import org.specs2.{ScalaCheck, Specification}

import org.scalacheck.Prop._

import commbank.coppersmith.Arbitraries._
import commbank.coppersmith.util.{Time, Date}

object TimeSpec extends Specification with ScalaCheck { def is = s2"""
  Parse valid date string $parseDate
  Parse valid time string $parseTime
  Print valid date string $printDate
  Print valid time string $printTime
"""

  def parseDate = forAll { (dateTime: DateTime) => {
    val dateStr = dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
    val date    = Date.parse(dateStr)

    date must_== Date(dateTime.getYear, dateTime.getMonthOfYear, dateTime.getDayOfMonth)
  }}

  def parseTime = forAll { (dateTime: DateTime) => {
    val dateStr = dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ"))
    val time    = Time.parse(dateStr)
    val offsetL = dateTime.getZone.getOffset(dateTime)

    time must_== Time(
      dateTime.getYear,
      dateTime.getMonthOfYear,
      dateTime.getDayOfMonth,
      dateTime.getHourOfDay,
      dateTime.getMinuteOfHour,
      dateTime.getSecondOfMinute,
      dateTime.getMillisOfSecond,
      Some((MILLISECONDS.toHours(offsetL).toInt,
            Math.abs(MILLISECONDS.toMinutes(offsetL).toInt % 60))
      )
    )
  }}

  def printDate = forAll { (date: Date) => {
    val dateStr  = date.toString
    val dateTime = DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate(dateStr)

    dateTime must_== new LocalDate(date.year, date.month, date.day)
  }}

  def printTime = forAll { (time: Time) => {
    val timeStr  = time.toString
    val dateTime = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
      .withOffsetParsed().parseDateTime(timeStr)
    val (h, m)   = time.offset.getOrElse((0,0))
    val offset   = DateTimeZone.forOffsetHoursMinutes(h, m)
    if (time.offset.isEmpty) timeStr must contain("-00:00")

    dateTime must_== new DateTime(
      time.year,
      time.month,
      time.day,
      time.hour,
      time.minute,
      time.second,
      time.millisecond,
      offset
    )
  }}


}
