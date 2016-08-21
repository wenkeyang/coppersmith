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

object TestUtil {
  // This is quite general and could probably be lifted up to one of the dependency projects
  def withoutLogging[T](loggerNames: String*)(f: =>T): T = {
    import org.apache.log4j.{Level, Logger}

    val loggerLevels: Iterable[(Logger, Level)] = loggerNames.map { loggerName =>
      val logger = Logger.getLogger(loggerName)
      val loggerLevel = logger.getLevel
      (logger, loggerLevel)
    }

    loggerLevels.foreach { case (logger, _) => logger.setLevel(Level.OFF) }

    try {
      f
    } finally {
      loggerLevels.foreach { case (logger, level) => logger.setLevel(level) }
    }
  }
}
