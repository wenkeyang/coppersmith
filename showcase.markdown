SHOWCASE
========

## Post-aggregation filters (having)

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Rating

object PopularRatingCountFeatures extends AggregationFeatureSet[Rating] {
  ...

  val popularRatingCount = select(count(_.rating > 3)).having(_ > 10)
    .asFeature(Continuous, "MOVIE_POPULAR_RATING_COUNT",
               "High rating count for movies that have more than 10 high ratings")
  ...
}
```

## Rank aggregators (maxBy, minBy)

```scala
object JoinFeatures extends AggregationFeatureSet[(Movie, Rating)] {
...
  val highestRatingUser = select(maxBy(_._2.rating)(_._2.userId))
    .asFeature(Nominal, "HIGHEST_RATING_USER",
               "User id for the user's highest rating")
...
}
```

## Pre-join filters (where, distinct on Sources)

```scala
case class ComedyJoinFeaturesConfig(conf: Config) extends FeatureJobConfig[(Movie, Rating)] {
  val comedyMovies   = HiveTextSource[Movie, Nothing](
    new Path("data/movies"), Partitions.unpartitioned).where(_.isComedy)
  ...
}
```
