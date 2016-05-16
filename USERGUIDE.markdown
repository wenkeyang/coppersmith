Coppersmith User Guide
======================

This is a guide to using the Coppersmith library to define and generate features.
It is aimed at programmers and data engineers.
The [**Basics**](#basics) section introduces the fundamental concepts
and should be read in full.
The [**Intermediate**](#intermediate) section is also highly recommended.
The [**Advanced**](#advanced) section serves as a reference
to additional features of the library.
The [**Try It Yourself**](#try-it-yourself) section contains details of how to run
some Coppersmith examples yourself using publicly available data.

In addition to this document, there is also a
[troubleshooting guide](TROUBLESHOOTING.markdown) available.


Basics
------


### Getting started

Add the coppersmith plugin to your SBT configuration. That is, inside
`project/plugins.sbt`,

    addSbtPlugin("au.com.cba.omnia" %% "coppersmith-plugin" % "<coppersmith-version>")

, where `<coppersmith-version>` is replaced with the version number of
coppersmith you want to use. The plugin adds the appropriate coppersmith versions
to your build and enables the publishing of feature metadata.


### The `Feature` class

An individual feature is represented by the `Feature[S, V]` type.
The two type parameters, `S` and `V`, are the *source* (or input)
type and the *value* (or output) type respectively.
You can think of it as a function from `S` to `V`:
- The source type is typically a thrift struct,
  describing the schema of a source table.
- The value type is one of `Integral`, `Decimal`, or `Str`.

A feature must also define some metadata, including:
- a feature *namespace*,
- a feature *name*, and
- a feature *type* (`Continuous`, `Discrete`, `Ordinal` or `Nominal`).

Below is an example of a feature defined by extending the `Feature` class.
If this looks complicated, don't worry!
In the next section,
we'll see how this can be made a lot easier.

```scala
// NOTE: This example is for pedagogical purposes only; it is not the
// recommended approach. Consider using the convenience methods of a
// FeatureSet subclass or the featureBuilder API (see below for both).

package commbank.coppersmith.examples.userguide

import scala.util.Try

import java.util.Locale

import org.joda.time.format.DateTimeFormat

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Movie

object MovieReleaseYear extends Feature[Movie, Integral](
  Metadata[Movie, Integral](namespace      = "userguide.examples",
                            name           = "MOVIE_RELEASE_YEAR",
                            description    = "Calendar year in which the movie was released",
                            featureType    = Continuous)
) {
  val format = DateTimeFormat.forPattern("dd-MMM-yyyy").withLocale(Locale.ENGLISH)

  def generate(movie: Movie) =
    Try(format.parseDateTime(movie.releaseDate)).toOption.map(v =>
        FeatureValue(entity = movie.id, name = "MOVIE_RELEASE_YEAR", value = v.getYear))
}
```

Note that returning `None` in `generate` will result in no feature being generated.
This is done here to handle incorrectly formatted release dates.
Check [filtering](#filtering-aka-where) and [source views](#source-views) for alternatives.

### The `FeatureSet` class

`FeatureSet[S]` is a group of features
derived from the same source record type `S`.
There are several predefined subtypes
such as `BasicFeatureSet`, `PivotFeatureSet`, and `AggregationFeatureSet`.
Extending one of these classes is the recommended way of creating features,
since it avoids duplicating the definition of namespace, entity, etc.
Even if you only want to create one feature,
it is still better to start with a `FeatureSet`,
and use the convenience methods such as `basicFeature`
to express the feature logic.

A feature set must define a property called `features`
which is simply an iterable collection of `Feature` objects.
The recommended pattern is to define each individual feature as a `val`,
using a convenience method such as `basicFeature`,
and then set the `features` property to a `List` of these vals.
Typically the compiler can infer most of the types,
including the feature value type
(for example `Int` and `Long` are promoted to the `Integral` type).
Type inference allows for a cleaner feature definition.

Here is an example using `BasicFeatureSet`
(which provides the method `basicFeature`).
For details of the other classes available, refer to the **Advanced** section.

```scala
package commbank.coppersmith.examples.userguide

import scala.util.Try

import java.util.Locale

import org.joda.time.format.DateTimeFormat

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Movie

object MovieFeatures extends BasicFeatureSet[Movie] {
  val namespace              = "userguide.examples"
  def entity(movie: Movie)   = movie.id
  val format = DateTimeFormat.forPattern("dd-MMM-yyyy").withLocale(Locale.ENGLISH)

  val movieReleaseYear = basicFeature[Integral](
    "MOVIE_RELEASE_YEAR", "Calendar year in which the movie was released", Continuous,
    (movie) => Try(format.parseDateTime(movie.releaseDate)).toOption.map(_.getYear)
  )

  val features = List(movieReleaseYear)
}
```

Note that, as opposed to above, returning `None` as part of `basicFeature` will
generate a feature with a value of `null`.

### Execution: the `SimpleFeatureJob` class

Coppersmith is part of a broader programme of work
that aims to simplify all the repetitive aspects of feature generation,
including deployment and execution.
However, we are not there yet!
For the moment,
you still need to create a mainline scalding job
to execute the features which you have defined,
and deploy it in the usual way.

To make this as easy as possible,
Coppersmith provides a class called `SimpleFeatureJob`,
which helps turn a `FeatureSet` into a
[maestro](https://github.com/CommBank/maestro) job.
As with any maestro job,
you still need to define the `job` function,
but in many cases
this is as simple as calling the `generate` function
with a `FeatureSet` and a `FeatureJobConfig`.

Recall that the definition of the features themselves
says nothing about where on disk to read the data.
`FeatureJobConfig` is where you specify
the *source* and *sink*
for the feature generation job.
Coppersmith currently supports the following source types:

- `HiveTextSource`: delimited plain text files
  under a [Hive](https://hive.apache.org/)-partitioned directory structure
- `HiveParquetSource`: [Parquet](https://parquet.apache.org/)-encoded
  files under a Hive-partitioned directory structure
- `TypedPipeSource`: see [Generating features from custom scalding code](#generating-features-from-custom-scalding-code)

and one sink type:

- `EavtTextSink`: writes EAVT records to a Hive-partitioned directory
structure using delimited text encoding

Here is an example of a job which materialises the feature set
which we defined in the previous section:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

case class MovieFeaturesConfig(conf: Config) extends FeatureJobConfig[Movie] {
  val partitions     = Partitions.unpartitioned
  val movies         = HiveTextSource[Movie, Nothing](new Path("data/movies"), partitions)

  val featureSource  = From[Movie]().bind(from(movies))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val dbPrefix       = conf.getArgs("db-prefix")
  val dbRoot         = new Path(conf.getArgs("db-root"))
  val tableName      = conf.getArgs("table-name")

  val featureSink    = EavtTextSink.configure(dbPrefix, dbRoot, tableName)
}

object MovieFeaturesJob extends SimpleFeatureJob {
  def job = generate(MovieFeaturesConfig(_), MovieFeatures)
}
```

Individual data sources that are common to different feature sources can be
pulled up to their own type for reuse. For an example of this, see the
`DirectorDataSource` in [Generating Features from Custom Scalding Code](#generating-features-from-custom-scalding-code).

### Partition selection

The source data used so far is not partitioned. In the below example, the
hive text source is partitioned by year, based on `Movie.ReleaseDate`.

The job will execute on all movies released in the year of the `generation-datetime`
specified in the config.

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

case class PartitionedMovieFeaturesConfig(conf: Config) extends FeatureJobConfig[Movie] {
  val generationDateTimeStr = conf.getArgs("generation-datetime")
  val generationDateTime    = DateTime.parse(generationDateTimeStr)

  val partition             = HivePartition.byYear(Fields[Movie].ReleaseDate, "dd-MMM-yyyy")
  val partitions            = Partitions(partition, generationDateTime.getYear.toString)
  val movies                = HiveTextSource[Movie, String](new Path("data/movies"), partitions)

  val featureSource         = From[Movie]().bind(from(movies))

  val featureContext        = ExplicitGenerationTime(generationDateTime)

  val dbPrefix              = conf.getArgs("db-prefix")
  val dbRoot                = new Path(conf.getArgs("db-root"))
  val tableName             = conf.getArgs("table-name")
  val featureSink           = EavtTextSink.configure(dbPrefix, dbRoot, tableName)
}

object PartitionedMovieFeaturesJob extends SimpleFeatureJob {
  def job = generate(PartitionedMovieFeaturesConfig(_), MovieFeatures)
}
```

For partitioned tables, you can list more than one partition.
Additionally, you can use any glob/wildcard pattern that Hadoop supports.
For example:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

object MultiPartitionSnippet {
  type Partition = (String, String, String) // Year, Month, Day

  // Last two days of July, and all of August
  val partition  = HivePartition.byDay(Fields[Movie].ReleaseDate, "dd-MMM-yyyy")
  val partitions =
    Partitions(partition, ("2015", "07", "30"), ("2015", "07", "31"), ("2015", "08", "*"))

  val movies     = HiveTextSource[Movie, Partition](new Path("data/movies"), partitions)
}
```

### Alternate sinks

If an output format different to `EavtTextSink` is required, then `TextSink`
should be used. A `Thrift` struct defining the sink format is needed, as well as
an implicit implementation of `FeatureValueEnc` for the `Thrift` struct.
For example, this is a simple implementation where only the column names
are different.

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.{FeatureEavt, Movie}

case class AlternativeSinkMovieFeaturesConfig(conf: Config) extends FeatureJobConfig[Movie] {
  implicit object FeatureEavtEnc extends FeatureValueEnc[FeatureEavt] {
    def encode(fvt: (FeatureValue[_], Time)): FeatureEavt = fvt match {
      case (fv, time) =>
        val featureValue = (fv.value match {
          case Integral(v) => v.map(_.toString)
          case Decimal(v) => v.map(_.toString)
          case Str(v) => v
        }).getOrElse(TextSink.NullValue)

        val featureTime = new DateTime(time).toString("yyyy-MM-dd")
        FeatureEavt(fv.entity, fv.name, featureValue, featureTime)
    }
  }

  val partitions     = Partitions.unpartitioned
  val movies         = HiveTextSource[Movie, Nothing](new Path("data/movies"), partitions)

  val featureSource  = From[Movie]().bind(from(movies))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val dbPrefix       = conf.getArgs("db-prefix")
  val dbRoot         = new Path(conf.getArgs("db-root"))
  val tableName      = conf.getArgs("table-name")

  val sinkPartition  = DerivedSinkPartition[FeatureEavt, (String, String, String)](
                         HivePartition.byDay(Fields[FeatureEavt].FeatureTime, "yyyy-MM-dd")
                       )
  val featureSink    = TextSink.configure(dbPrefix, dbRoot, tableName, sinkPartition)
}

object AlternativeSinkMovieFeaturesJob extends SimpleFeatureJob {
  def job = generate(AlternativeSinkMovieFeaturesConfig(_), MovieFeatures)
}
```

Note: When using `TextSink`, a `SinkPartition` is required.

Intermediate
------------

This section introduces an alternative API for feature definitions.


### Shaping input data: the `FeatureSource`

In the example above,
we quietly snuck in `From[Movie]()` without explanation.
The type of this expression is `FeatureSource`.
This is a trivial example,
since it simply indicates that
the input source is a stream of `Movie` records.
But, as will be described in the **Advanced** section,
this also provides the basis for complex joins.

Since details such as the join condition
belong together with the feature definition,
it is good practice to define a `FeatureSource`
*as a property* of each `FeatureSet`.
By convention, we will call it `source`.
For an example, see the following section.


### A fluent API: `featureBuilder`

Coppersmith offers an alternate "fluent" interface
which results in more readable feature definitions
reminiscent of SQL.
Unless you have special requirements
(e.g. extending `FeatureSet` with your own convenience methods)
then the fluent API is generally preferred.

In the example below,
notice that we create a val called `select`.
This is the key to accessing the fluent API.
We then treat `select` as a function,
using it to define our features.
Finally, we call `asFeature` to specify the feature metadata,
returning a `Feature` object.

```scala
package commbank.coppersmith.examples.userguide

import scala.util.Try

import java.util.Locale

import org.joda.time.format.DateTimeFormat

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

object FluentMovieFeatures extends FeatureSetWithTime[Movie] {
  val namespace            = "userguide.examples"
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
```

The above example uses `FeatureSetWithTime`, which, by default, uses the feature
context to give us the feature time. This implementation can be overridden like so:
`def time(movie: Movie, ctx: FeatureContext)   = DateTime.parse(movie.releaseDate).getMillis`
Most of the time however, the default implementation is the correct thing to do.

Advanced
--------

### Tip: Extension methods for thrift structs

If you find yourself repeating certain calculations
(such as the date parsing in previous examples),
you may find that defining a "rich" version of the thrift struct
can help to keep feature definitions clear and concise.

```scala
package commbank.coppersmith.examples.userguide

import scala.util.Try

import java.util.Locale

import org.joda.time.{DateTime, Period}
import org.joda.time.format.DateTimeFormat

import commbank.coppersmith.examples.thrift.{Movie, Rating}

object Implicits {
  implicit class RichMovie(movie: Movie) {
    val format                            = DateTimeFormat.forPattern("dd-MMM-yyyy").withLocale(Locale.ENGLISH)
    def safeReleaseDate: Option[DateTime] = Try(format.parseDateTime(movie.releaseDate)).toOption
    def releaseYear:     Option[Int]      = safeReleaseDate.map(_.getYear)
    def isComedy:        Boolean          = movie.comedy == 1
    def isFantasy:       Boolean          = movie.fantasy == 1
    def isAction:        Boolean          = movie.action == 1
    def isScifi:         Boolean          = movie.scifi == 1

    def ageAt(date: DateTime): Option[Int] = safeReleaseDate.map(new Period(_, date).getYears)
  }

  implicit class RichRating(rating: Rating) {
    def eventYear: Int = DateTime.parse(rating.timestamp).getYear
  }
}
```

Subsequent examples will use concise syntax
such as `_.releaseYear` wherever possible. Note that some genre
classifications have also been included for use in future examples.


### Pivoting

The simplest kind of feature is simply the value of a field.
This pattern is known as "pivoting",
because it transforms a wide input record
into the narrow EAVT format.

The `PivotFeatureSet` provides the convenience method `pivot()`, which
allows you to pivot single fields. Note the use of `Maestro.Fields` to
specify the field of the thrift struct.

```scala
package commbank.coppersmith.examples.userguide

import au.com.cba.omnia.maestro.api.Maestro.Fields

import commbank.coppersmith.api._

import commbank.coppersmith.examples.thrift.Movie

object MoviePivotFeatures extends PivotFeatureSet[Movie] {
  val namespace = "userguide.examples"
  def entity(m: Movie) = m.id

  val source = From[Movie]()

  val title:       Feature[Movie, Str]      = pivot(Fields[Movie].Title,       "Movie title",        Nominal)
  val imdbUrl:     Feature[Movie, Str]      = pivot(Fields[Movie].ImdbUrl,     "Movie IMDb URL",     Nominal)
  val releaseDate: Feature[Movie, Str]      = pivot(Fields[Movie].ReleaseDate, "Movie release date", Nominal)
  val action:      Feature[Movie, Integral] = pivot(Fields[Movie].Action,      "Movie is action",    Discrete)

  def features = List(title, imdbUrl, releaseDate, action)
}
```


### Aggregation (aka `GROUP BY`)

By subclassing `AggregationFeatureSet` and using `FeatureBuilder`,
you gain access to a number of useful aggregate functions:
`count`, `avg`, `max`, `min`, `maxBy`, `minBy`, `sum`, and `uniqueCountBy`.
These are convenience methods for creating
[Algebird `Aggregator`s](https://github.com/twitter/scalding/wiki/Aggregation-using-Algebird-Aggregators).
Other aggregators can be defined by providing your own
`Aggregator` instance (see the `ratingStdDev` feature in the example that
follows).

The grouping criteria (in SQL terms, the `GROUP BY` clause)
is implicitly *the entity and the time*,
as defined by the `entity` and `time` properties.

Note that when using `AggregationFeatureSet`,
you should *not* override `features`;
provide `aggregationFeatures` instead. Also, there is no
option to specify time as a function of the source record,
so the time will always come from the job's `FeatureContext`.

Here is an example that finds the average rating per movie.

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Rating

object RatingFeatures extends AggregationFeatureSet[Rating] {
  val namespace              = "userguide.examples"
  def entity(rating: Rating) = rating.movieId

  val source = From[Rating]()
  val select = source.featureSetBuilder(namespace, entity)

  val avgRating = select(avg(_.rating))
    .asFeature(Continuous, "MOVIE_AVG_RATING",
               "Average movie rating")

  import com.twitter.algebird.{Aggregator, Moments}
  val stdDev: Aggregator[Double, Moments, Double] =
    Moments.aggregator.andThenPresent(_.stddev)

  val ratingStdDev = select(stdDev.composePrepare[Rating](_.rating))
    .asFeature(Continuous, "RATING_STANDARD_DEVIATION",
      "Standard deviation of the ratings per movie")

  val aggregationFeatures = List(avgRating, ratingStdDev)
}
```

<a name="aggregator-source-view-note" />
Note that when using Aggregators with [source views](#source-views), the
`Aggregator` must be specified explicitly instead of using the inherited
aggregate functions. This is because the inherited functions are tied directly
to the `AggregationFeatureSet` source type. See the
[note](#source-view-aggregator-note) in the source views guide for an example
of this.

Also note that non-aggregation features cannot be defined in an `AggregationFeatureSet`.
In order to keep similar features organised, two `FeatureSet` objects
should be created, and called using one `FeatureJob`.

In the below example, because there are two feature sets that have different
sources (`Movie` and `Rating`), it's necessary to create two `FeatureJobConfig`
objects. If the two `FeatureSet` objects used the same source and wrote to the
same sink, only one `FeatureJobConfig` would be required.

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Rating

object AggregationFeatures extends AggregationFeatureSet[Rating] {
  val namespace              = "userguide.examples"
  def entity(rating: Rating) = rating.movieId

  val source = From[Rating]()
  val select = source.featureSetBuilder(namespace, entity)

  val avgRating = select(avg(_.rating))
    .asFeature(Continuous, "MOVIE_AVG_RATING",
               "Average movie rating")
  val aggregationFeatures = List(avgRating)
}
```

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

object NonAggregationFeatures extends FeatureSetWithTime[Movie] {
  val namespace            = "userguide.examples"
  def entity(movie: Movie) = movie.id

  val source = From[Movie]()
  val select = source.featureSetBuilder(namespace, entity)

  val movieTitle = select(_.title)
    .asFeature(Nominal, "MOVIE_TITLE",
               "Movie title")
  val features = List(movieTitle)
}
```

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.{Movie, Rating}

trait CommonConfig {
  def conf: Config

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
  val featureSink    = EavtTextSink.configure("userguide", new Path("dev"), "movies")
}

case class AggregationFeaturesConfig(conf: Config)
    extends FeatureJobConfig[Rating] with CommonConfig {

  val partitions     = Partitions.unpartitioned
  val ratings        = HiveTextSource[Rating, Nothing](new Path("data/ratings"), partitions, "\t")
  val featureSource  = From[Rating]().bind(from(ratings))
}

case class NonAggregationFeaturesConfig(conf: Config)
    extends FeatureJobConfig[Movie] with CommonConfig {
  val partitions     = Partitions.unpartitioned
  val movies         = HiveTextSource[Movie, Nothing](new Path("data/movies"), partitions)
  val featureSource  = From[Movie]().bind(from(movies))
}

object AggregationFeaturesJob extends SimpleFeatureJob {
  def job = generate(AggregationFeaturesConfig(_), AggregationFeatures)
}

object NonAggregationFeaturesJob extends SimpleFeatureJob {
  def job = generate(NonAggregationFeaturesConfig(_), NonAggregationFeatures)
}

object CombinedFeaturesJob extends SimpleFeatureJob {
  def job = generate(
    FeatureSetExecutions(
      FeatureSetExecution(AggregationFeaturesConfig(_), AggregationFeatures),
      FeatureSetExecution(NonAggregationFeaturesConfig(_), NonAggregationFeatures)
    )
  )
}
```

### Post-aggregation filters (aka `HAVING`)

Sometimes we want to filter the aggregation value (equivalent to an SQL `HAVING`
clause. This is accomplished using the `having` method on the aggregator. The 
following example outputs the rating count where the count is greater that 10:


```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Rating

object PopularRatingCountFeatures extends AggregationFeatureSet[Rating] {
  val namespace              = "userguide.examples"
  def entity(rating: Rating) = rating.movieId

  val source = From[Rating]()
  val select = source.featureSetBuilder(namespace, entity)

  val popularRatingCount = select(count(_.rating > 3)).having(_ > 10)
    .asFeature(Continuous, "MOVIE_POPULAR_RATING_COUNT",
               "High rating count for movies that have more than 10 high ratings")

  val aggregationFeatures = List(popularRatingCount)
}
```


### Filtering (aka `WHERE`)

Features need not be defined for every input value.
When defining features using the fluent API,
one or more filters can be added using the `where` method
(`andWhere` is also a synonym,
to improve readability when there are multiple conditions).

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

import Implicits.RichMovie

object MovieReleaseFeatures extends FeatureSetWithTime[Movie] {
  val namespace            = "userguide.examples"
  def entity(movie: Movie) = movie.id

  val source = From[Movie]()
  val select = source.featureSetBuilder(namespace, entity)

  val comedyMovieReleaseYears = select(_.releaseYear)
    .where(_.isComedy)
    .asFeature(Continuous, "COMEDY_MOVIE_RELEASE_YEAR",
               "Release year for comedy movies")

  val recentFantasyReleaseYears = select(_.releaseYear)
    .where   (_.isFantasy)
    .andWhere(_.releaseYear.exists(_ >= 1990))
    .asFeature(Continuous, "RECENT_FANTASY_MOVIE_RELEASE_YEAR",
               "Release year for fantasy movies released in or after 1990")

  val features = List(comedyMovieReleaseYears, recentFantasyReleaseYears)
}
```

If a filter is common to all features from a single source,
the filter can be defined on the source itself to prevent
repetition in the feature definitions.

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

import Implicits.RichMovie

object HollywoodGoldenEraMovieFeatures extends FeatureSetWithTime[Movie] {
  val namespace              = "userguide.examples"
  def entity(cust: Movie)    = cust.id

  // Common filter applied to all features built from this source
  val source = From[Movie]().filter(c => c.releaseYear.exists(Range(1920, 1965).contains(_)))
  val select = source.featureSetBuilder(namespace, entity)

  val goldenEraComedyTitles = select(_.title)
    .where(_.isComedy)
    .asFeature(Nominal, "GOLDEN_ERA_COMEDY_MOVIE_TITLE",
               "Title of comedy movies released between 1920 and 1965")

  val goldenEraActionTitles = select(_.title)
    .where(_.isAction)
    .asFeature(Nominal, "GOLDEN_ERA_ACTION_MOVIE_TITLE",
               "Title of action movies released between 1920 and 1965")

  val features = List(goldenEraComedyTitles, goldenEraActionTitles)
}
```

In the special case where the value is always the same,
but the filter varies,
consider using the `QueryFeatureSet`:

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Movie

import Implicits.RichMovie

object MovieGenreFlags extends QueryFeatureSet[Movie, Str] {
  val namespace            = "userguide.examples"
  def entity(movie: Movie) = movie.id

  def value(movie: Movie)  = "Y"
  val featureType          = Nominal

  val source = From[Movie]()

  val comedyMovie = queryFeature("MOVIE_IS_COMEDY", "'Y' if movie is comedy", _.isComedy)
  val fantasyMovie = queryFeature("MOVIE_IS_ACTION", "'Y' if movie is action", _.isAction)

  val features = List(comedyMovie, fantasyMovie)
}
```


### Joins

At the type level,
a feature calculated from two joined tables
has a pair of thrift structs as its source,
e.g. `(Movie, Rating)`.
For a left join, the value on the right may be missing,
e.g. `(Movie, Option[Rating])`.

Self-joins are possible, yielding `(Movie, Movie)` for an inner self join,
and `(Movie, Option[Movie])` for an left outer self join.

As described in the section **Shaping input data**,
the convention of defining a `source` per feature set
allows you to specify further detail about the join:

- Use `Join[A].to[B]` for an inner join
- Use `Join.left[A].to[B]` for a left outer join
- Use `.on(left: A => J, right: A => J)` to specify the join columns

An example might be the average rating for comedy movies:

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.{Movie, Rating}

import Implicits.RichMovie

object JoinFeatures extends AggregationFeatureSet[(Movie, Rating)] {
  val namespace                  = "userguide.examples"
  def entity(s: (Movie, Rating)) = s._1.id

  val source = Join[Movie].to[Rating].on(
    movie   => movie.id,
    rating  => rating.movieId
  )
  val select = source.featureSetBuilder(namespace, entity)

  val averageRatingForComedyMovies = select(avg(_._2.rating))
    .where(_._1.isComedy)
    .asFeature(Continuous, "COMEDY_MOVIE_AVG_RATING",
               "Average rating for comedy movies")

  val aggregationFeatures = List(averageRatingForComedyMovies)
}
```

In the accompanying `FeatureJobConfig` class,
bind the source using either `join` or `leftJoin`, as appropriate.
For example:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.{Movie, Rating}

case class JoinFeaturesConfig(conf: Config) extends FeatureJobConfig[(Movie, Rating)] {
  val movies = HiveTextSource[Movie, Nothing](new Path("data/movies"), Partitions.unpartitioned)
  val ratings = HiveTextSource[Rating, Nothing](new Path("data/ratings"), Partitions.unpartitioned, "\t")

  val featureSource  = JoinFeatures.source.bind(join(movies, ratings))
  val featureSink    = EavtTextSink.configure("userguide", new Path("dev"), "ratings")
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
}

object JoinFeaturesJob extends SimpleFeatureJob {
  def job = generate(JoinFeaturesConfig(_), JoinFeatures)
}
```

### Left join

An example of a left join is getting the count of movies, grouped by director. As our
movie data are from a different source (the source of directors can be found
[here](#generating-features-from-custom-scalding-code)), not all directors will match a movie.

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

object LeftJoinFeatures extends AggregationFeatureSet[(Director, Option[Movie])] {
  val namespace                            = "userguide.examples"
  def entity(s: (Director, Option[Movie])) = s._1.name

  val source = Join.left[Director].to[Movie].on(
    director => director.movieTitle,
    movie    => movie.title
  )
  val select = source.featureSetBuilder(namespace, entity)

  // Count the number of records where a matching movie was found
  val directorMovieCount = select(count(!_._2.isEmpty))
    .asFeature(Continuous, "DIRECTOR_MOVIE_COUNT",
               "Count of movies directed")

  val aggregationFeatures = List(directorMovieCount)
}

case class LeftJoinFeaturesConfig(conf: Config) extends FeatureJobConfig[(Director, Option[Movie])] {
  val movies    = HiveTextSource[Movie, Nothing](new Path("data/movies"), Partitions.unpartitioned)
  val directors = DirectorSourceConfig.dataSource

  val featureSource  = LeftJoinFeatures.source.bind(leftJoin(directors, movies))
  val featureSink    = EavtTextSink.configure("userguide", new Path("dev"), "directors")
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
}

object LeftJoinFeaturesJob extends SimpleFeatureJob {
  def job = generate(LeftJoinFeaturesConfig(_), LeftJoinFeatures)
}
```

### Multiway joins

Joins between more than two tables are also possible, using the `multiway` function.

For example, the average rating of each sci fi movie from engineers:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, TypedPipe}

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.{Movie, Rating, User}

import Implicits.RichMovie

object MultiJoinFeatures extends AggregationFeatureSet[(Movie, Rating, User)] {
  val namespace                        = "userguide.examples"
  def entity(s: (Movie, Rating, User)) = s._1.id

  val source = Join.multiway[Movie]
      .inner[Rating].on((movie: Movie)               => movie.id,
                         (rating: Rating)            => rating.movieId)
      .inner[User].on((movie: Movie, rating: Rating) => rating.userId,
                         (user: User)                => user.id)
      .src  // Note the use of the .src call. Awkward implementation detail

  val select = source.featureSetBuilder(namespace, entity)

  val avgRatingForSciFiMoviesFromEngineeringUsers= select(avg(_._2.rating))
    .where(_._3.occupation == "engineer")
    .andWhere(_._1.isScifi)
    .asFeature(Continuous, "SCIFI_MOVIE_AVG_RATING_FROM_ENGINEERS",
               "Average rating for movie that is scifi, where users' occupations are 'engineer'")

  val aggregationFeatures = List(avgRatingForSciFiMoviesFromEngineeringUsers)
}


case class MultiJoinFeaturesConfig(conf: Config) extends FeatureJobConfig[(Movie, Rating, User)] {
  val movies: DataSource[Movie, TypedPipe]    = HiveTextSource[Movie, Nothing](new Path("data/movies"),
                                                  Partitions.unpartitioned)
  val ratings: DataSource[Rating, TypedPipe]  = HiveTextSource[Rating, Nothing](new Path("data/ratings"),
                                                  Partitions.unpartitioned, "\t")
  val users: DataSource[User, TypedPipe]      = HiveTextSource[User, Nothing](new Path("data/users"),
                                                  Partitions.unpartitioned)

  val featureSource  = MultiJoinFeatures.source.bind(joinMulti((movies, ratings, users), MultiJoinFeatures.source))
  val featureSink    = EavtTextSink.configure("userguide", new Path("dev"), "ratings")
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
}

object MultiJoinFeaturesJob extends SimpleFeatureJob {
  def job = generate(MultiJoinFeaturesConfig(_), MultiJoinFeatures)
}
```

Notice that for multiway joins, we need to hint the types of the join functions
to the compiler. Each stage of the join needs two functions: one producing
a join column for all of the left values so far, and one for the current right
value.


### Source views

Sometimes it is convenient to work with a different view of
the source when defining features. This might simply be a
mapping to a different type, or a pattern-based filter and
extraction. This is akin to Scala's `map` and `collect`
methods on the standard collections, and the same methods
are available on the `FeatureSetBuilder`.

The following example only generates IMDb path features for
movies where IMDb URL is defined (ie, not `None`). Notice how
even though the `Movie.imdbUrl` type is `Option[String]`,
there is no need to unwrap the option in the select clause
as it has already been extracted as part of matching against
`Some` in the `collect`.

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

object SourceViewFeatures extends FeatureSetWithTime[Movie] {
  val namespace              = "userguide.examples"
  def entity(movie: Movie)   = movie.id

  val source  = From[Movie]()
  val builder = source.featureSetBuilder(namespace, entity)

  val movieImdbPath =
    builder.map(m => m.imdbUrl).collect { case Some(imdbUrl) => imdbUrl }
      .select(_.replace("http://us.imdb.com/", ""))
      .asFeature(Nominal, "MOVIE_IMDB_PATH_AVAILABLE", "Movie IMDb Path when it is known")

  val features = List(movieImdbPath)
}
```

<a name="source-view-aggregator-note" />
As [noted](#aggregator-source-view-note) in the Aggregation features section,
when combining Aggregation features with source views, if the type of the source
view differs from the underlying feature set source type, the inherited
aggregator functions can no longer be used and the aggregator must be explicitly
specified.

In the example below, the feature set source type is (Movie, Rating), but the
source view has a type of Double. Therefore it is not possible to use the
preferred syntax avg(_). Instead, use AveragedValue.aggregator.

```scala
package commbank.coppersmith.examples.userguide

import com.twitter.algebird.AveragedValue

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.{Movie, Rating}

import Implicits.RichMovie

object SourceViewAggregationFeatures extends AggregationFeatureSet[(Movie, Rating)] {
  val namespace                      = "userguide.examples"
  def entity(s: (Movie, Rating))     = s._1.id

  val source  = Join[Movie].to[Rating].on(
    movie  => movie.id,
    rating => rating.movieId
  )

  val builder = source.featureSetBuilder(namespace, entity)

  val avgComedyRatings =
    builder.collect { case (movie, rating) if movie.isComedy => rating.rating.toDouble }
      .select(AveragedValue.aggregator)
      .asFeature(Continuous, "COMEDY_MOVIE_AVERAGE_RATING", "Average rating for comedy movies")

  val aggregationFeatures = List(avgComedyRatings)
}
```


### Pre-join Filters (experimental)

When using joined tables,
filters expressed using `.where` in the feature definition
are applied to the full joined tuple.
But it is sometimes desirable to filter the records before joining them,
mainly for performance reasons.

Currently, coppersmith offers pre-join filters for scalding datasources only.
This necessarily means that they must be specified in the the `Config`,
rather than the `FeatureSet`. Simply call `.where` on the datasource,
before binding it.

For example, consider the COMEDY_MOVIE_AVG_RATING from an earlier example.
This contains a one-to-many join,
which will return the rating for _every_ movie,
only to then discard everything except comedies.
Of course, if we expanded this FeatureSet with similar features for all genres,
then this approach would be quite efficient.
But suppose we really only care about comedies.
Then it might be more efficient to apply the filter before the join, as follows:

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.{Movie, Rating}

import Implicits.RichMovie

object ComedyJoinFeatures extends AggregationFeatureSet[(Movie, Rating)] {
  val namespace                  = "userguide.examples"
  def entity(s: (Movie, Rating)) = s._1.id

  val source = Join[Movie].to[Rating].on(
    movie   => movie.id,
    rating  => rating.movieId
  )
  val select = source.featureSetBuilder(namespace, entity)

  // Here we no longer need: .where(_._1.isComedy)
  val averageRatingForComedyMovies = select(avg(_._2.rating))
    .asFeature(Continuous, "COMEDY_MOVIE_AVG_RATING_V2",
               "Average rating for comedy movies")

  val aggregationFeatures = List(averageRatingForComedyMovies)
}

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api.scalding._

case class ComedyJoinFeaturesConfig(conf: Config) extends FeatureJobConfig[(Movie, Rating)] {
  val comedyMovies   = HiveTextSource[Movie, Nothing](new Path("data/movies"), Partitions.unpartitioned)
                       .where(_.isComedy)
  val ratings        = HiveTextSource[Rating, Nothing](new Path("data/ratings"), Partitions.unpartitioned, "\t")

  val featureSource  = ComedyJoinFeatures.source.bind(join(comedyMovies, ratings))
  val featureSink    = EavtTextSink.configure("userguide", new Path("dev"), "ratings")
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
}

object ComedyJoinFeaturesJob extends SimpleFeatureJob {
  def job = generate(ComedyJoinFeaturesConfig(_), ComedyJoinFeatures)
}
```


### Generating values from job context

Sometimes it is necessary to use job specific data for generating feature
values, for example, using date information from the job configuration to
calculate the age of a movie from its release date at the time of
generating a feature.

This can be achieved by incorporating a context with the `FeatureSet`'s source.

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

import Implicits.RichMovie

// The context, a DateTime in this case, forms part of the FeatureSource
object ContextFeatures extends FeatureSetWithTime[(Movie, DateTime)] {
  val namespace                    = "userguide.examples"
  def entity(s: (Movie, DateTime)) = s._1.id

  // Incorporate context with FeatureSource as a type
  val source = From[Movie].withContext[DateTime]

  val select = source.featureSetBuilder(namespace, entity)

  def movieAgeFeature =
    select(mdt => mdt._1.ageAt(mdt._2))
      .asFeature(Ordinal, "MOVIE_AGE", "Age of movie")

  val features = List(movieAgeFeature)
}
```

The context is passed through at the time of binding the concrete `DataSource`(s).

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.{DateTime, format}, format.DateTimeFormat

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

case class ContextFeaturesConfig(conf: Config)
    extends FeatureJobConfig[(Movie, DateTime)] {

  val partitions = Partitions.unpartitioned
  val movies     = HiveTextSource[Movie, Nothing](new Path("data/movies"), partitions)

  val date = conf.getArgs.optional("date").map(d =>
               DateTime.parse(d, DateTimeFormat.forPattern("yyyy-MM-dd"))
             ).getOrElse(new DateTime().minusDays(1))

  // Note: Current date passed through as context param
  val featureSource  = ContextFeatures.source.bindWithContext(from(movies), date)

  val featureSink    = EavtTextSink.configure("userguide", new Path("dev"), "movies")

  val featureContext = ExplicitGenerationTime(date)
}

object ContextFeaturesJob extends SimpleFeatureJob {
  val job = generate(ContextFeaturesConfig(_), ContextFeatures)
}


```


### Generating features from custom scalding code

Coppersmith adopts the 80/20 rule,
aiming to allow the most common feature patterns to be expressed
in a concise way.

For less common scenarios,
it may be necessary to directly implement the core logic
using the underlying execution engine (e.g. scalding).
Going down this route ties you to a specific backend,
so you lose some of the future-proofing that coppersmith provides,
but you can still retain some benefits by using coppersmith
to add metadata and manage the serialisation of the feature values.

As long as your calculation can be expressed as a `TypedPipe[T]`
(such as `directors` in the following example)
you can bind it to the feature source.
An SQL analogy would be using a view, in place of a concrete table.

This example uses a `foldLeft` and pattern matching to parse the strange format of directors.list.
The data used for this example can be found at
http://www.imdb.com/interfaces

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, TextLine}
import com.twitter.scalding.typed.TypedPipe

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.scalding.TypedPipeSource
import commbank.coppersmith.examples.thrift.{Movie, Rating}


case class Director(name: String, movieTitle: String)

object DirectorFeatures extends AggregationFeatureSet[(Director, Movie, Rating)] {
  val namespace                            = "userguide.examples"
  def entity(s: (Director, Movie, Rating)) = s._1.name

  val source = Join.multiway[Director]
    .inner[Movie].on((director: Director)      => director.movieTitle,
                    (movie: Movie)             => movie.title)
    .inner[Rating].on((d: Director, m: Movie)  => m.id,
                      (rating: Rating)         => rating.movieId)
    .src

  val select = source.featureSetBuilder(namespace, entity)

  val directorAvgRating = select(avg(_._3.rating))
    .asFeature(Continuous, "DIRECTOR_AVG_RATING", "Average rating of all movies the director has directed")

  val aggregationFeatures = List(directorAvgRating)
}

object DirectorSourceConfig {
  val dirPipe         = TypedPipe.from(TextLine("data/directors"))

  val directorPattern = "^([^\t]+)\t+([^\t]+)$".r
  val moviePattern    = "^\t+([^\t]+)$".r

  val filteredPipe = dirPipe.filter((line: String) => line.contains("\t") && line.matches(""".*\([\d\?]{4}.*"""))

  val directors: TypedPipe[Director] = filteredPipe.groupAll.foldLeft(List[Director]()) { (acc: List[Director], line: String) =>
    line match {
      case directorPattern(director, movie) => Director(director, movie) :: acc
      case moviePattern(movie) => Director(acc.head.name, movie) :: acc
      case _ => acc
    }
  }.values.flatten

  val dataSource: TypedPipeSource[Director] = TypedPipeSource(directors)
}

case class DirectorFeaturesConfig(conf: Config) extends FeatureJobConfig[(Director, Movie, Rating)] {

  val movies:  DataSource[Movie, TypedPipe]  = HiveTextSource[Movie, Nothing](new Path("data/movies"),
                                                 Partitions.unpartitioned)
  val ratings: DataSource[Rating, TypedPipe] = HiveTextSource[Rating, Nothing](new Path("data/ratings"),
                                                 Partitions.unpartitioned, "\t")

  val source = DirectorFeatures.source

  val directorsSource: DataSource[Director, TypedPipe] = DirectorSourceConfig.dataSource

  val featureSource  = source.bind(joinMulti((directorsSource, movies, ratings), source))
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
  val featureSink    = EavtTextSink.configure("userguide", new Path("dev"), "directors")
}

object DirectorFeaturesJob extends SimpleFeatureJob {
  def job = generate(DirectorFeaturesConfig(_), DirectorFeatures)
}
```


### Testing

Guidelines for unit testing are still forthcoming.

Try it yourself
---------------

You can build all of the examples found in this user
guide by running the SBT task `examples/assembly`.

All `FeatureSet`s without explicitly defined configs/jobs
have had those built with the correct naming convention.
E.g. `HollywoodGoldenEraMovieFeatures` has an accompanying
`HollywoodGoldenEraMovieFeaturesConfig` and `HollywoodGoldenEraMovieFeaturesJob`.

### Setup

In order to run the example you first need to obtain the famous Movie Lens data set that was used
in the Netflix challenge, and the list of directors from IMDb.

http://grouplens.org/datasets/movielens/

http://www.imdb.com/interfaces

unzip the ml-100k.zip in your project directory under data/ml-100k/

unzip directors.list.gz in your project directory under data/

Then if you, like me, are running Hadoop on Vagrant (there are easy instructions on how to do that
here
https://blog.cloudera.com/blog/2014/06/how-to-install-a-virtual-apache-hadoop-cluster-with-vagrant-and-cloudera-manager/)
you would do something similar to:

As user hdfs in my case:
```
hdfs dfs -mkdir -p data/ratings
hdfs dfs -mkdir data/movies
hdfs dfs -mkdir data/users
hdfs dfs -mkdir data/directors
hdfs dfs -chmod -R a+rwx data
```

As user vagrant:
```
hdfs dfs -copyFromLocal /vagrant/data/ml-100k/u.item data/movies/
hdfs dfs -copyFromLocal /vagrant/data/ml-100k/u.data data/ratings/
hdfs dfs -copyFromLocal /vagrant/data/ml-100k/u.user data/users/
hdfs dfs -copyFromLocal /vagrant/data/directors.list data/directors/
```


In my case as this is a fresh cluster I had to create the home directory for the vagrant user
(as user hdfs):
```
hdfs dfs -mkdir /user/vagrant
hdfs dfs -chown vagrant /user/vagrant
```

### Running the example

In order to run an example you will need to do something like:
```
hadoop jar /vagrant/examples/target/scala-2.11/coppersmith-examples-assembly-0.6.4-20160322022447-7096042-SNAPSHOT.jar commbank.coppersmith.examples.userguide.DirectorFeaturesJob --hdfs
```

You can then inspect the output on hdfs under `dev/view/warehouse/features/directors/year=2015/month=01/day=01/`
