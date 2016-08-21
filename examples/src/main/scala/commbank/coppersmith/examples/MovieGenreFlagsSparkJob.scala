package commbank.coppersmith.examples

import org.apache.hadoop.fs.Path

import org.joda.time.DateTime

import commbank.coppersmith.api._, spark._, Coppersmith._
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.examples.thrift.Movie

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import commbank.coppersmith.examples.userguide.MovieGenreFlags

case class MovieGenreFlagsConfig(implicit spark: SparkSession) extends FeatureJobConfig[Movie] {
  println("In MovieGenreFlagsConfig")
  val movies         = HiveTextSource[Movie](new Path("data/movies"))
  println(movies)
  val featureSource  = MovieGenreFlags.source.bind(from(movies))
  println(featureSource)
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
  println(featureContext)
  val featureSink    = SparkHiveSink[Eavt]("userguide", new Path("dev/movies"), "movies")
  println(featureSink)
}

object MovieGenreFlagsSparkJob extends SimpleFeatureJob {
  def job = generate((spark: SparkSession) => MovieGenreFlagsConfig()(spark), MovieGenreFlags)
}
