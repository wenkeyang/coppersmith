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

package commbank.coppersmith.examples.userguide

import au.com.cba.omnia.maestro.api.Maestro.DerivedDecode

import commbank.coppersmith.Feature.EntityId
import commbank.coppersmith.api._
import commbank.coppersmith.api.scalding._
import commbank.coppersmith.examples.thrift.{Movie, Rating}
import commbank.coppersmith.BoundFeatureSource
import commbank.coppersmith.scalding.FeatureSink

import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.Config
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
