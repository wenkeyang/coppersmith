package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.api.scalding._
import commbank.coppersmith.examples.thrift.Movie

case class PivotFeaturesConfig(conf: Config) extends FeatureJobConfig[Movie] {
  val partitions     = Partitions.unpartitioned
  val movies         = HiveTextSource[Movie, Nothing](new Path("data/movies"), partitions)

  val featureSink    = EavtSink.configure("userguide", new Path("dev"), "movies")

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val featureSource  = From[Movie].bind(from(movies))
}

object PivotFeaturesJob extends SimpleFeatureJob {
  val job = generate(PivotFeaturesConfig(_), PivotMacroExample.moviePivotFeatures)
}
