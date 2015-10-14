package commbank.coppersmith

import org.joda.time.DateTime

trait FeatureContext {
  def generationTime: DateTime
}

case class ParameterisedFeatureContext(generationTime: DateTime) extends FeatureContext