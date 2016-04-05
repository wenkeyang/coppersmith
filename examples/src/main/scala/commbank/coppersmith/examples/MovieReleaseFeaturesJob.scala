package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api.Maestro.DerivedDecode

import commbank.coppersmith.api._, scalding._
import commbank.coppersmith.examples.thrift.Movie

case class MovieReleaseFeaturesConfig(conf: Config) extends FeatureJobConfig[Movie] {
  val partitions     = ScaldingDataSource.Partitions.unpartitioned
  val movies         = HiveTextSource[Movie, Nothing](new Path("data/movies"), partitions)

  val featureSource  = MovieReleaseFeatures.source.bind(from(movies))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val featureSink    = EavtSink.configure("userguide", new Path("dev"), "movies")
}

object MovieReleaseFeaturesJob extends SimpleFeatureJob {
  def job = generate(MovieReleaseFeaturesConfig(_), MovieReleaseFeatures)
}
