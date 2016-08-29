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

  /** This is intended for use in Thermometer specs, to avoid the logs which parquet spews out.
    *
    * It only needs to be called once in the test run (although multiple calls are not harmful).
    * The recommended usage is to place it in your test class's static initializer block.
    *
    * All java.util.logging output will be squashed, with special handling to ensure that parquet does not
    * later reverse this. As long as everything in the stack that you care about is using a different
    * logging framework (e.g. SLF4J), then you probably won't mind this overkill to silence parquet.
    */
  def withoutJavaLogging() = {
    import java.util.logging.Logger

    // Suppress parquet logging (which wraps java.util.logging) in tests. Loading the parquet.Log
    // class forces its static initialiser block to be run prior to removing the root logger
    // handlers, which needs to be done to avoid the Log's default handler being re-added. Disabling
    // log output specific to the parquet logger would be more ideal, however, this has proven to be
    // non-trivial due to what appears to be a result of configuration via static initialisers and
    // the order in which the initialisers are called.
    // FIXME: This can be replaced by TestUtil.withoutLogging when slf4j is available in parquet
    // via https://github.com/apache/parquet-mr/pull/319
    Logger.getLogger(getClass.getName).info("Suppressing further java.util.logging log output")
    Class.forName("parquet.Log")
    val rootLogger = Logger.getLogger("")
    rootLogger.getHandlers.foreach(rootLogger.removeHandler)
  }
}
