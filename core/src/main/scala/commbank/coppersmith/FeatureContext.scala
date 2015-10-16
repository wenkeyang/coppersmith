package commbank.coppersmith

import org.joda.time.DateTime

trait FeatureContext {
  def generationTime: DateTime
}

case class ExplicitGenerationTime(generationTime: DateTime) extends FeatureContext