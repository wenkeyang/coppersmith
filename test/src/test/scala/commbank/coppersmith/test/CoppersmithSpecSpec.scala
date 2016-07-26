package commbank.coppersmith.test

import commbank.coppersmith.api._, Coppersmith._
import org.specs2.Specification


object CoppersmithSpecSpec extends Specification with CoppersmithSpecSupport {
  def is = s2"""
     Can define a coppersmith specification for a basic feature set $basicSpec
     Can define a coppersmith specification for an aggregation feature set $aggregationSpec
    """

  def basicSpec = {
    import DemoFeatureSet._

    val data = List(
      Movie("M1", "Lord of the Flies"),
      Movie("M2", "The Truman Show"),
      Movie("M3", "Jurrasic Park")
    )
    Seq(
      feature(movieTitle).onData(data).values must contain(Str(Some("Lord of the Flies"))),
      feature(movieTitle).onData(data).valueFor("M2") === Some(Str(Some("The Truman Show")))
    )
  }

  def aggregationSpec = {
    import DemoFeatureSet2._

    //Awkward
    implicit val featureSet = DemoFeatureSet2

    val movieData = List(
      Movie("M1", "Lord of the Flies"),
      Movie("M2", "The Truman Show"),
      Movie("M3", "Jurrasic Park")
    )

    val ratingData = List(
      Rating("u1", "M1", 4),
      Rating("u2", "M1", 3),
      Rating("u2", "M2", 3),
      Rating("u3", "M2", 8)
    )

    val data = joins.liftJoinInner(featureSet.source)(movieData, ratingData)

    feature(maxRating).onData(data).valueFor("M2") === Some(Integral(Some(8)))
  }
}

case class Movie(id: String, title:String)
case class Rating(userId: String, movieId: String, rating: Int)

object DemoFeatureSet extends FeatureSetWithTime[Movie] {
  def namespace = "test"
  def entity(m: Movie) = m.id

  val source = From[Movie]
  val select = source.featureSetBuilder(namespace, entity)

  def movieTitle = select(_.title).asFeature(Nominal, "MOVIE_TITLE", "Movie title")

  def features = List(movieTitle)
}

object DemoFeatureSet2 extends AggregationFeatureSet[(Movie, Rating)] {
  def namespace = "test"
  def entity(m: (Movie, Rating)) = m._1.id

  val source = Join[Movie].to[Rating].on(
    movie   => movie.id,
    rating  => rating.movieId
  )
  val select = source.featureSetBuilder(namespace, entity)

  def maxRating = select(max(_._2.rating)).asFeature(Nominal, "MAX_RATING", "MAX_RATING")

  def aggregationFeatures = List(maxRating)
}