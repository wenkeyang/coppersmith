package commbank.coppersmith.examples

import org.apache.hadoop.fs.Path

import org.joda.time.DateTime

import commbank.coppersmith.api._, spark._, Coppersmith._
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.examples.thrift.Movie

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import commbank.coppersmith.examples.userguide.MovieGenreFlags

case class MovieGenreFlagsConfig(spark: SparkSession) extends FeatureJobConfig[Movie] {
  val movies         = HiveTextSource[Movie](new Path("data/movies"))

  val featureSource  = MovieGenreFlags.source.bind(from(movies))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val featureSink    = SparkHiveSink[Eavt]("userguide", new Path("dev/movies"), "movies")
}

object MovieGenreFlagsSparkJob extends SimpleFeatureJob {
  def job = generate((spark: SparkSession) => MovieGenreFlagsConfig(spark.sparkContext.getConf)(spark), MovieGenreFlags)
}
