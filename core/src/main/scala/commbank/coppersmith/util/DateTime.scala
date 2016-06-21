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

package commbank.coppersmith.util

import java.util.concurrent.TimeUnit.MILLISECONDS

import commbank.coppersmith.util.Timestamp.Offset

import scalaz.Order

case class DatePeriod(years: Int, months: Int, days: Int)

object Datestamp {
  def parse(date: String): Datestamp = {
    import org.joda.time.format.DateTimeFormat

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val d   = fmt.parseLocalDate(date)
    Datestamp(d.getYear, d.getMonthOfYear, d.getDayOfMonth)
  }

  def parse(date: String, pattern: String): Datestamp = {
    import org.joda.time.format.DateTimeFormat

    val fmt = DateTimeFormat.forPattern(pattern)
    val d   = fmt.parseLocalDate(date)
    Datestamp(d.getYear, d.getMonthOfYear, d.getDayOfMonth)
  }

  implicit def ordering[A <: Datestamp]: Ordering[A] = Ordering.by(_.toString)
  implicit def scalazOrder[A <: Datestamp]: Order[A] = Order.fromScalaOrdering(ordering)
}

object Timestamp {
  type Offset = Option[(Int, Int)]

  /**
    * Parses a timestamp in RFC3339 format with millisecond precision.
    * @param time The timestamp to parse
    * @return The timestamp
    */

  def parseWithMillis(time: String): Timestamp = {
    val offset = parseOffset(time)
    parsePatternWithOffset(time, "yyyy-MM-dd'T'HH:mm:ss.SSSZZ", offset)
  }

  /**
    * Parses a timestamp in RFC3339 format without millisecond precision.
    * @param time The timestamp to parse
    * @return The timestamp
    */

  def parseWithoutMillis(time: String): Timestamp = {
    val offset = parseOffset(time)
    parsePatternWithOffset(time, "yyyy-MM-dd'T'HH:mm:ssZZ", offset)
  }

  /**
    * Parses a timestamp with a pattern. Note: The pattern must parse timezone information.
    * @param time The timestamp to parse
    * @param pattern The pattern to use (Must parse timezone)
    * @return The timestamp
    */

  def parsePattern(time: String, pattern: String): Timestamp = {
    import org.joda.time.format.DateTimeFormat

    // Remove literals
    val p = pattern.replaceAll("'[^']*'", "")
    if (!p.contains("Z")) throw new IllegalArgumentException("Pattern must parse timezones.")

    val fmt = DateTimeFormat.forPattern(pattern)
    val dt  = fmt.withOffsetParsed.parseDateTime(time)
    val tz  = dt.getZone.getOffset(dt)

    val offset = Some((MILLISECONDS.toHours(tz).toInt,
      Math.abs(MILLISECONDS.toMinutes(tz).toInt % 60)))

    Timestamp(dt.getMillis, offset)
  }

  /**
    * Parses a timestamp and sets offset. Use to provide offset information missing from th
    * timestamp, or to overwrite offset information. Note: The time will not be adjusted to the new
    * offset, the existing offset will be replaced.
    * @param time The timestamp to parse
    * @param pattern The pattern to use to parse
    * @param offset The offset to set
    * @return The timestamp
    */

  def parsePatternWithOffset(time: String, pattern: String, offset: Offset): Timestamp = {
    import org.joda.time.format.DateTimeFormat
    import org.joda.time.DateTimeZone

    val (h, m) = offset.getOrElse((0,0))
    val tz     = DateTimeZone.forOffsetHoursMinutes(h, m)
    val fmt    = DateTimeFormat.forPattern(pattern)
    // Without withOffsetParsed the timezone fields are moved to system timezone
    val dt     = fmt.withOffsetParsed().parseDateTime(time).withZoneRetainFields(tz)

    Timestamp(dt.getMillis, offset)
  }

  private def parseOffset(time: String): Option[(Int, Int)] = {
    // Parse timezone hour and minute
    val tzParser =
      """.*([\-+]\d{2}):(\d{2}).*""".r

    // -00:00 represents unknown timezone
    val offset = time match {
      case tzParser("-00", "00") => None
      case tzParser(h, m) => Some((h.toInt, m.toInt))
      case _ => None
    }
    offset
  }

  implicit def ordering[A <: Timestamp]: Ordering[A] = Ordering.by(_.toUTC.toString)
  implicit def scalazOrder[A <: Timestamp]: Order[A] = Order.fromScalaOrdering(ordering)
}

case class Datestamp(year: Int, month: Int, day: Int) {
  protected def toLocalDate: org.joda.time.LocalDate ={
    import org.joda.time.LocalDate

    new LocalDate(year, month, day)
  }

  def difference(that: Datestamp): DatePeriod = that match {
    case Datestamp(y, m, d) =>
      import org.joda.time.Period
      val p = new Period(this.toLocalDate, that.toLocalDate)
      DatePeriod(p.getYears, p.getMonths, p.getDays)
  }

  override def toString: String = {
    f"$year%04d-$month%02d-$day%02d"
  }
}

case class Timestamp(millis: Long, offset: Offset) {

  def toUTC: Timestamp = {
    import org.joda.time.DateTimeZone

    val dt = toDateTime.toDateTime(DateTimeZone.UTC)
    Timestamp.parseWithMillis(dt.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ"))
  }

  protected def toDateTime: org.joda.time.DateTime ={
    import org.joda.time.{DateTimeZone, DateTime}

    val (h, m) = offset.getOrElse((0,0))
    val tz = DateTimeZone.forOffsetHoursMinutes(h, m)
    new DateTime(millis, tz)
  }

  override def toString: String = {
    val offsetStr = offset.map { case ((h, m)) => f"$h%+03d:$m%02d" }.getOrElse("-00:00")
    f"${toDateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS")}$offsetStr"
  }
}

