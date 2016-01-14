package commbank.coppersmith.examples.userguide

import au.com.cba.omnia.maestro.api.Maestro.DerivedDecode

import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{Config, Execution, TypedTsv}

import commbank.coppersmith.Feature.Value.{Decimal, Integral, Str}
import commbank.coppersmith.Feature.{EntityId, Time}
import commbank.coppersmith.api._
import commbank.coppersmith.api.scalding._
import commbank.coppersmith.examples.thrift.{Movie, Rating}
import commbank.coppersmith.scalding.FeatureSink
import commbank.coppersmith.{BoundFeatureSource, FeatureValue}

import org.apache.hadoop.fs.Path
import org.joda.time.DateTime

case class MovieFeaturesConfig(conf: Config) extends FeatureJobConfig[(Movie, Rating)] {
  val partitions    = ScaldingDataSource.Partitions.unpartitioned
  val ratings       = HiveTextSource[Rating, Nothing](new Path("/data/rating"), partitions, "\t")
  val movies        = HiveTextSource[Movie, Nothing](new Path("/data/movie"), partitions)

  override def featureSink: FeatureSink = FlatFeatureSink("/data/output")

  override def featureContext: FeatureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  override def featureSource: BoundFeatureSource[(Movie, Rating), TypedPipe] = MovieFeatures.source.bind(join(movies, ratings))
}

case class FlatFeatureSink(output: String) extends FeatureSink {
  override def write(features: TypedPipe[(FeatureValue[_], Time)]): Execution[Unit] = {

    val featurePipe = features.map { case (fv, t) =>
      val featureValue = (fv.value match {
        case Integral(v) => v.map(_.toString)
        case Decimal(v)  => v.map(_.toString)
        case Str(v)      => v
      }).getOrElse("")
      s"${fv.entity}|${fv.name}|${featureValue}"
    }
    featurePipe.writeExecution(TypedTsv[String](output)).unit
  }
}

object MovieFeatures extends AggregationFeatureSet[(Movie, Rating)]{
  override def entity(s: (Movie, Rating)): EntityId = s._1.title

  override def aggregationFeatures = List(averageRating)

  override def namespace = "movielens"

  val source = Join[Movie].to[Rating].on(
    movie   => movie.id,
    rating  => rating.movieId
  )

  val select = source.featureSetBuilder(namespace, entity)
  val averageRating = select(avg(_._2.rating)).asFeature(Continuous, "AVERAGE_MOVIE_RATING", "Average movie rating")
}

object MovieFeaturesJob extends SimpleFeatureJob {
  def job = generate(MovieFeaturesConfig(_), MovieFeatures)
}