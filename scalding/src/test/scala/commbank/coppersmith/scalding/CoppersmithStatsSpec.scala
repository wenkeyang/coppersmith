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

package commbank.coppersmith.scalding

import scala.collection.JavaConversions._

import com.twitter.scalding.TypedPipe

import uk.org.lidalia.slf4jtest.TestLoggerFactory
import uk.org.lidalia.slf4jtest.LoggingEvent.info

import au.com.cba.omnia.thermometer.core._

import CoppersmithStats.fromTypedPipe

object CoppersmithStatsSpec extends ThermometerSpec { def is = s2"""
    Stats are logged in a sensible order  $stats
  """

  def stats = {
    val tp = TypedPipe.from(List(1,2,3)).withCounter("tp")
    val exec = CoppersmithStats.logCountersAfter(tp.toIterableExecution)
    executesSuccessfully(exec) must_== List(1,2,3)

    val logger = TestLoggerFactory.getTestLogger("commbank.coppersmith.scalding.CoppersmithStats")
    logger.getAllLoggingEvents().toList must_== List(
      info("Coppersmith counters:"                        ),
      info("    tp                                      3")
    )
  }
}
