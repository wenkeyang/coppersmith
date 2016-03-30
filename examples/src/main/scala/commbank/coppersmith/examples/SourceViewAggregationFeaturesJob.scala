package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path
import com.twitter.scalding.Config
import au.com.cba.omnia.maestro.api.{HivePartition, Maestro}
import Maestro.{DerivedDecode, Fields}
import org.joda.time.DateTime
import commbank.coppersmith.api._, scalding._
import commbank.coppersmith.examples.thrift.{Movie, Rating}

case class SourceViewAggregationFeaturesConfig(conf: Config) extends FeatureJobConfig[(Movie, Rating)] {
  val partitions     = ScaldingDataSource.Partitions.unpartitioned
  val movies = HiveTextSource[Movie, Nothing](new Path("data/movies"), partitions)
  val ratings = HiveTextSource[Rating, Nothing](new Path("data/ratings"), partitions, "\t")

  val featureSource  = SourceViewAggregationFeatures.source.bind(join(movies, ratings))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val featureSink    = EavtSink.configure("userguide", new Path("dev"), "movies")
}

object SourceViewAggregationFeaturesJob extends SimpleFeatureJob {
  def job = generate(SourceViewAggregationFeaturesConfig(_), SourceViewAggregationFeatures)
}
