//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith.plugin.example

import commbank.coppersmith.api._, Coppersmith._

import scala.util.Try
import java.util.Locale
import org.joda.time.format.DateTimeFormat

case class Movie(id: String, releaseDate: String)

object FluentMovieFeatures extends FeatureSetWithTime[Movie] {
  val namespace            = "plugin.examples"
  def entity(movie: Movie) = movie.id

  val source      = From[Movie]()  // FeatureSource (see above)
  val select      = source.featureSetBuilder(namespace, entity)

  val format      = DateTimeFormat.forPattern("dd-MMM-yyyy").withLocale(Locale.ENGLISH)

  val movieReleaseDay  = select(_.releaseDate)
    .asFeature(Nominal, "MOVIE_RELEASE_DAY", "Day on which the movie was released")
  val movieReleaseYear = select(movie => Try(format.parseDateTime(movie.releaseDate)).toOption.map(_.getYear))
    .asFeature(Continuous, "MOVIE_RELEASE_YEAR", "Calendar year in which the movie was released")

  val features = List(movieReleaseDay, movieReleaseYear)
}

object FluentMovieFeatures2 extends FeatureSetWithTime[Movie] {
  val namespace            = "plugin.examples"
  def entity(movie: Movie) = movie.id

  val source      = From[Movie]()  // FeatureSource (see above)
  val select      = source.featureSetBuilder(namespace, entity)

  val format      = DateTimeFormat.forPattern("dd-MMM-yyyy").withLocale(Locale.ENGLISH)

  val movieReleaseDay2  = select(_.releaseDate)
    .asFeature(Nominal, "MOVIE_RELEASE_DAY2", "Day on which the movie was released - take 2")


  val features = List(movieReleaseDay2)
}
