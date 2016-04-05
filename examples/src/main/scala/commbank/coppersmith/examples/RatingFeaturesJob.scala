package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api.Maestro.DerivedDecode

import commbank.coppersmith.api._, scalding._
import commbank.coppersmith.examples.thrift.Rating

case class RatingFeaturesConfig(conf: Config) extends FeatureJobConfig[Rating] {
  val partitions     = ScaldingDataSource.Partitions.unpartitioned
  val ratings        = HiveTextSource[Rating, Nothing](new Path("data/ratings"), partitions, "\t")

  val featureSource  = RatingFeatures.source.bind(from(ratings))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val featureSink    = EavtSink.configure("userguide", new Path("dev"), "ratings")
}

object RatingFeaturesJob extends SimpleFeatureJob {
  def job = generate(RatingFeaturesConfig(_), RatingFeatures)
}
