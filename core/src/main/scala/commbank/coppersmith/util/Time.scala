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

object Date {
  def parse(date: String): Date = {
    import org.joda.time.format.DateTimeFormat

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val d   = fmt.parseLocalDate(date)
    Date(d.getYear, d.getMonthOfYear, d.getDayOfMonth)
  }
}
object Time {
  def parse(date: String): Time = {
    import org.joda.time.format.{DateTimeFormat, DateTimeFormatterBuilder}

    val patterns = Array("yyyy-MM-dd'T'HH:mm:ss.SSSZZ", "yyyy-MM-dd'T'HH:mm:ssZZ")
    val fmts     = patterns map (DateTimeFormat.forPattern(_).getParser)
    val fmt      = new DateTimeFormatterBuilder().append(null, fmts).toFormatter
    val dt       = fmt.withOffsetParsed.parseDateTime(date)

    // Parse timezone hour and minute
    val tzParser = """.*([\-+]\d{2}):(\d{2}).*""".r

    // -00:00 represents unknown timezone
    val offset = date match {
      case tzParser("-00", "00") => None
      case tzParser(h, m)        => Some((h.toInt, m.toInt))
      case _                     => None
    }

    Time(
      dt.getYear,
      dt.getMonthOfYear,
      dt.getDayOfMonth,
      dt.getHourOfDay,
      dt.getMinuteOfHour,
      dt.getSecondOfMinute,
      dt.getMillisOfSecond,
      offset
    )
  }
}

case class Date(year: Int, month: Int, day: Int) {
  override def toString(): String = {
    f"$year%04d-$month%02d-$day%02d"
  }
}

case class Time(year: Int,
                month: Int,
                day: Int,
                hour: Int,
                minute: Int,
                second: Int,
                millisecond: Int,
                offset: Option[(Int, Int)]
               ) {
  override def toString: String = {
    val offsetStr = (offset map {
      case (h, m) => f"$h%+03d:$m%02d"
    }).getOrElse("-00:00")
    f"$year%04d-$month%02d-$day%02dT$hour%02d:$minute%02d:$second%02d.$millisecond%03d$offsetStr"
  }
}
