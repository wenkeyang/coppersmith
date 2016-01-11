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
