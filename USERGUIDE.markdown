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

* [Basics](#basics)
    * [Getting started](#getting-started)
    * [The Feature class](#the-feature-class)
    * [The FeatureSet class](#the-featureset-class)
    * [Execution: the SimpleFeatureJob class](#execution-the-simplefeaturejob-class)
    * [Partition selection](#partition-selection)
* [Intermediate](#intermediate)
    * [Shaping input data: the FeatureSource](#shaping-input-data-the-featuresource)
    * [A fluent API: featureBuilder](#a-fluent-api-featurebuilder)
* [Advanced](#advanced)
    * [Tip: Extension methods for thrift structs](#tip-extension-methods-for-thrift-structs)
    * [Pivoting](#pivoting)
    * [Aggregation (aka GROUP BY)](#aggregation-aka-group-by)
    * [Post-aggregation filters (aka HAVING)](#post-aggregation-filters-aka-having)
    * [Filtering (aka WHERE)](#filtering-aka-where)
    * [Joins](#joins)
    * [Left join](#left-join)
    * [Multiway joins](#multiway-joins)
    * [Source views](#source-views)
    * [Generating values from job context](#generating-values-from-job-context)
    * [Alternate sinks](#alternate-sinks)
    * [Generating features from custom scalding code](#generating-features-from-custom-scalding-code)
    * [Blank features](#blank-features)
    * [Testing](#testing)
* [Try it yourself](#try-it-yourself)
    * [Setup](#setup)
    * [Running the example](#running-the-example)


Basics
------


### Getting started

Add the coppersmith plugin to your SBT configuration. That is, inside
`project/plugins.sbt`,

    addSbtPlugin("au.com.cba.omnia" %% "coppersmith-plugin" % "<coppersmith-version>")

, and inside `build.sbt`,

    libraryDependencies ++= Seq("au.com.cba.omnia" %% "coppersmith-core"     % "<coppersmith-version>",
                                "au.com.cba.omnia" %% "coppersmith-scalding" % "<coppersmith-version>",
                                "au.com.cba.omnia" %% "coppersmith-tools"    % "<coppersmith-version>")

, where `<coppersmith-version>` is replaced with the version number of
coppersmith you want to use. The plugin enables the publishing of feature metadata.


### The `Feature` class

An individual feature is represented by the `Feature[S, V]` type.
The two type parameters, `S` and `V`, are the *source* (or input)
type and the *value* (or output) type respectively.
You can think of it as a function from `S` to `V`:
- The source type is typically a [thrift](https://thrift.apache.org/) struct,
  describing the schema of a source table.
- The value type is one of `Integral`, `Decimal`, `FloatingPoint`, `Str`, `Date` or `Time`.

A feature must also define some metadata, including:
- a feature *namespace*,
- a feature *name*,
- a feature *description*,
- a feature *featureType* (`Continuous`, `Discrete`, `Ordinal`, `Nominal` or `Instant`) and optionally
- a feature *range* (`MinMaxRange`, `SetRange` or `MapRange`).

`featureType` | Permitted `Value` types | Examples
--- | --- | ---
`Continuous` (Numeric) | `Integral`<br />`Decimal`<br />`FloatingPoint` | Height in *mm* (e.g. `1822`)<br />Gross Domestic Product (`18460646000000.0`)<br />Area of house (`87.2`)
`Discrete` (Numeric) | `Integral` | Number of visitors to a website (e.g. `35034859`)
`Ordinal` (Categorical) | `Integral`<br />`Decimal`<br />`FloatingPoint` <br /> `Str`|Customer satisfaction (e.g. `1` to `10`)<br /> <br /> Movie rating (e.g. `1.0`,`4.0`,`4.5` etc.) <br /> Flood risk (e.g. `"Low"`, `"Moderate"`, `"High"`)
`Nominal` (Categorical) | `Integral`<br />`Str`<br />`Bool` | Product ID (e.g. `22`, `31`, `50`)<br />Nationality (e.g. `"Australia"`, `"India"`, `"UK"`)<br />Joined in last 6 months? (`true` or `false`)
`Instant` (DateTime) | `Date`<br />`Time` | Date of birth (`"yyyy-MM-dd"`)<br />Transaction time (`"yyyy-MM-dd'T'HH:mm:ss.SSSZZ"`)
(See [variable descriptions](http://www.abs.gov.au/websitedbs/a3121120.nsf/home/statistical+language+-+what+are+variables) for further information.)

Below is an example of a feature defined by extending the `Feature` class.
If this looks complicated, don't worry!
In the next section,
we'll see how this can be made a lot easier.

```scala
// NOTE: This example is for pedagogical purposes only; it is not the
// recommended approach. Consider using the convenience methods of a
// FeatureSet subclass or the featureBuilder API (see below for both).

package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.User

object UserAge extends Feature[User, Integral](
  Metadata[User, Integral](namespace   = "userguide.examples",
                           name        = "USER_AGE",
                           description = "Age of user in years",
                           featureType = Continuous,
                           valueRange  = Some(MinMaxRange[Integral](0, 130)))
) {
  def generate(user: User) =
    Some(FeatureValue(entity = user.id, name = "USER_AGE", value = user.age))
}
```

Note that here we return `Some` in `generate`. If `None` was returned,
the feature would not be generated.

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

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.User

object UserFeatures extends BasicFeatureSet[User] {
  val namespace          = "userguide.examples"
  def entity(user: User) = user.id

  val userAge = basicFeature[Integral](
    "USER_AGE", "Age of user in years", Continuous, Some(MinMaxRange[Integral](0, 130))
  )((user) => user.age)

  val features = List(userAge)
}
```

Note that, as opposed to above, returning `None` from the function
passed to `basicFeature` will generate a feature with a value of `null`.

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

two sink types:

- `HiveTextSink`: writes records to a Hive-partitioned directory
structure using delimited text encoding
- `HiveParquetSink`: writes records to a Hive-partitioned directory
structure using Parquet encoding

and one sink thrift implementation:

- `EavtText`: contains an implicit encoder (`EavtEnc`) and a
 default partition (`eavtByDay`)

Here is an example of a job which materialises the feature set
which we defined in the previous section:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.examples.thrift.User

case class UserFeaturesConfig(conf: Config) extends FeatureJobConfig[User] {
  val partitions     = Partitions.unpartitioned
  val users          = HiveTextSource[User, Nothing](new Path("data/users"), partitions)

  val featureSource  = From[User]().bind(from(users))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val dbPrefix       = conf.getArgs("db-prefix")
  val dbRoot         = new Path(conf.getArgs("db-root"))
  val tableName      = conf.getArgs("table-name")

  val featureSink    = HiveTextSink[Eavt](dbPrefix, dbRoot, tableName, eavtByDay)
}

object UserFeaturesJob extends SimpleFeatureJob {
  def job = generate(UserFeaturesConfig(_), UserFeatures)
}
```

Individual data sources (e.g. `users`) that are common to different
feature sources can be pulled up to their own type for reuse.
For an example of this, see the
`DirectorDataSource` in [Generating Features from Custom Scalding Code](#generating-features-from-custom-scalding-code).

### Partition selection

The source data used so far is not partitioned. In the below example, the
hive text source is partitioned by `User.Zipcode`.

The job will execute on all users with a zipcode specified by `user-zipcode`
in the config.

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.examples.thrift.User

case class PartitionedUserFeaturesConfig(conf: Config) extends FeatureJobConfig[User] {
  val userZipcode    = conf.getArgs("user-zipcode")

  val partition      = HivePartition.byField(Fields[User].Zipcode)
  val partitions     = Partitions(partition, userZipcode)
  val users          = HiveTextSource[User, String](new Path("data/users"), partitions)

  val featureSource  = From[User]().bind(from(users))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val dbPrefix       = conf.getArgs("db-prefix")
  val dbRoot         = new Path(conf.getArgs("db-root"))
  val tableName      = conf.getArgs("table-name")
  val featureSink    = HiveTextSink[Eavt](dbPrefix, dbRoot, tableName, eavtByDay)
}

object PartitionedUserFeaturesJob extends SimpleFeatureJob {
  def job = generate(PartitionedUserFeaturesConfig(_), UserFeatures)
}
```

For partitioned tables, you can list more than one partition.
Additionally, you can use any glob/wildcard pattern that Hadoop supports.
For example:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.User

object MultiPartitionSnippet {
  // Spring Green and Houston
  val partition  = HivePartition.byField(Fields[User].Zipcode)
  val partitions = Partitions(partition, "53588", "770**", "772**")

  val users      = HiveTextSource[User, String](new Path("data/users"), partitions)
}
```


Intermediate
------------

This section introduces an alternative API for feature definitions.


### Shaping input data: the `FeatureSource`

In the example above,
we quietly snuck in `From[User]()` without explanation.
The type of this expression is `FeatureSource`.
This is a trivial example,
since it simply indicates that
the input source is a stream of `User` records.
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

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.User

object FluentUserFeatures extends FeatureSetWithTime[User] {
  val namespace          = "userguide.examples"
  def entity(user: User) = user.id

  val source   = From[User]()  // FeatureSource (see above)
  val select   = source.featureSetBuilder(namespace, entity)

  val userAge  = select(user => user.age)
    .asFeature(Continuous, "USER_AGE", Some(MinMaxRange(0, 130)),
      "Age of user in years")
    
  val userAgeGroup = select(user => user.age match {
    case x if x <= 14           => "Child"
    case x if x > 14 && x <= 24 => "Youth"
    case x if x > 24 && x <= 64 => "Adult"
    case x if x > 64            => "Senior"
  }).asFeature(Ordinal, "USER_AGE_GROUP", Some(SetRange("Child", "Youth", "Adult", "Senior")), 
               "Age group of user")

  val userIsEngineer = select(user => user.occupation == "engineer")
    .asFeature(Nominal, "USER_IS_ENGINEER", "Whether the user is an engineer")

  val occupationCode = select(user => user.occupation match {
      case "engineer"      => 15
      case "programmer"    => 28
      case "scientist"     => 44
      case "administrator" => 71
      case _               => 0
    }).asFeature(
      Nominal, 
      "OCCUPATION_CODE",
      Some(MapRange(
        15 -> "engineer",
        28 -> "programmer",
        44 -> "scientist",
        71 -> "administrator",
        0  -> "other"
      )),
      "Occupation code for user"
    )

  val features = List(userAge, userAgeGroup, userIsEngineer, occupationCode)
}
```

The above example uses `FeatureSetWithTime`, which, by default, uses the feature
context to give us the feature time. This implementation can be overridden like so:
`def time(user: User, ctx: FeatureContext) = DateTime.parse(user.DOB).getMillis`
Most of the time however, the default implementation is the correct thing to do.

Advanced
--------

### Tip: Extension methods for thrift structs

If you find yourself repeating certain calculations,
you may find that defining a "rich" version of the thrift struct
can help to keep feature definitions clear and concise.

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.util.Datestamp
import commbank.coppersmith.examples.thrift.Movie

object Implicits {
  implicit class RichMovie(movie: Movie) {
    val parse = Datestamp.parseFormat("dd-MMM-yyyy")

    def safeReleaseDate: Option[Datestamp] =
      movie.releaseDate.flatMap(parse(_).right.toOption)

    def releaseYear: Option[Int] = safeReleaseDate.map(_.year)
    def isAction:    Boolean     = movie.action == 1
    def isComedy:    Boolean     = movie.comedy == 1
    def isFantasy:   Boolean     = movie.fantasy == 1
    def isScifi:     Boolean     = movie.scifi == 1

    def ageAt(date: Datestamp): Option[Int] = safeReleaseDate.map(_.difference(date).years)
  }
}
```

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
  val action:      Feature[Movie, Integral] = pivot(Fields[Movie].Action,      "Movie is action",    Discrete, Some(SetRange[Integral](0, 1)))

  def features = List(title, imdbUrl, releaseDate, action)
}
```

Note that the `Fields` macro cannot be used to get "rich" thrift fields.

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
    .asFeature(Continuous, "MOVIE_AVG_RATING", Some(MinMaxRange(0.0, 5.0)),
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

In the example below, because there are two feature sets that have different
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
    .asFeature(Continuous, "MOVIE_AVG_RATING", Some(MinMaxRange(0.0, 5.0)),
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

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.examples.thrift.{Movie, Rating}

trait CommonConfig {
  def conf: Config

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
  val featureSink    = HiveTextSink[Eavt]("userguide", new Path("dev/movies"), "movies", eavtByDay)
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
clause). This is accomplished using the `having` method on the aggregator. The
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
    .asFeature(Discrete, "MOVIE_POPULAR_RATING_COUNT", 
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

  val comedyMovie = queryFeature("MOVIE_IS_COMEDY", "'Y' if movie is comedy", Some(SetRange("Y")))(_.isComedy)
  val fantasyMovie = queryFeature("MOVIE_IS_ACTION", "'Y' if movie is action", Some(SetRange("Y")))(_.isAction)

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
    .asFeature(Continuous, "COMEDY_MOVIE_AVG_RATING", Some(MinMaxRange(0.0, 5.0)),
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

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.examples.thrift.{Movie, Rating}

case class JoinFeaturesConfig(conf: Config) extends FeatureJobConfig[(Movie, Rating)] {
  val movies = HiveTextSource[Movie, Nothing](new Path("data/movies"), Partitions.unpartitioned)
  val ratings = HiveTextSource[Rating, Nothing](new Path("data/ratings"), Partitions.unpartitioned, "\t")

  val featureSource  = JoinFeatures.source.bind(join(movies, ratings))
  val featureSink    = HiveTextSink[Eavt]("userguide", new Path("dev/ratings"), "ratings", eavtByDay)
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

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
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
    .asFeature(Discrete, "DIRECTOR_MOVIE_COUNT",
               "Count of movies directed")

  val aggregationFeatures = List(directorMovieCount)
}

case class LeftJoinFeaturesConfig(conf: Config) extends FeatureJobConfig[(Director, Option[Movie])] {
  val movies    = HiveTextSource[Movie, Nothing](new Path("data/movies"), Partitions.unpartitioned)
  val directors = DirectorSourceConfig.dataSource

  val featureSource  = LeftJoinFeatures.source.bind(leftJoin(directors, movies))
  val featureSink    = HiveTextSink[Eavt]("userguide", new Path("dev/directors"), "directors", eavtByDay)
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

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.examples.thrift.{Movie, Rating, User}

import Implicits.RichMovie

object MultiJoinFeatures extends AggregationFeatureSet[(Movie, Rating, User)] {
  val namespace                        = "userguide.examples"
  def entity(s: (Movie, Rating, User)) = s._1.id

  val source = Join.multiway[Movie]
      .inner[Rating].on((movie: Movie)               => movie.id,
                        (rating: Rating)             => rating.movieId)
      .inner[User].on((movie: Movie, rating: Rating) => rating.userId,
                      (user: User)                   => user.id)

  val select = source.featureSetBuilder(namespace, entity)

  val avgRatingForSciFiMoviesFromEngineeringUsers= select(avg(_._2.rating))
    .where(_._3.occupation == "engineer")
    .andWhere(_._1.isScifi)
    .asFeature(Continuous, "SCIFI_MOVIE_AVG_RATING_FROM_ENGINEERS", Some(MinMaxRange(0.0, 5.0)),
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

  val featureSource  = MultiJoinFeatures.source.bind(joinMulti(movies, ratings, users))
  val featureSink    = HiveTextSink[Eavt]("userguide", new Path("dev/ratings"), "ratings", eavtByDay)
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

The following example only generates release year features for
movies where release date is defined (ie, not `None`), and in
a valid format. Notice how even though the `Movie.safeReleaseDate`
type is `Option[Datestamp]`, there is no need to unwrap the option in
the select clause as it has already been extracted as part of matching against
`Some` in the `collect`.

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.Movie

import Implicits.RichMovie

object SourceViewFeatures extends FeatureSetWithTime[Movie] {
  val namespace            = "userguide.examples"
  def entity(movie: Movie) = movie.id

  val source  = From[Movie]()
  val builder = source.featureSetBuilder(namespace, entity)

  val movieReleaseYear =
    builder.map(m =>
      m.safeReleaseDate
    ).collect {
      case Some(date) => date
    }.select(_.year)
     .asFeature(Continuous, "MOVIE_RELEASE_YEAR", "Movie Release year when it is known")

  val features = List(movieReleaseYear)
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
      .asFeature(Continuous, "COMEDY_MOVIE_AVERAGE_RATING", Some(MinMaxRange(0.0, 5.0)),
                 "Average rating for comedy movies")

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

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
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
    .asFeature(Continuous, "COMEDY_MOVIE_AVG_RATING_V2", Some(MinMaxRange(0.0, 5.0)),
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
  val featureSink    = HiveTextSink[Eavt]("userguide", new Path("dev/ratings"), "ratings", eavtByDay)
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
}

object ComedyJoinFeaturesJob extends SimpleFeatureJob {
  def job = generate(ComedyJoinFeaturesConfig(_), ComedyJoinFeatures)
}
```

If, rather than filtering, a distinct operation is required,
`ScaldingDataSource` also provides `.distinct` and `.distinctBy`.

Note: An implicit `Ordering` must be provided, either for the source type
when using `.distinct`; or for the type to distinct on when using `.distinctBy`.

For example:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.examples.thrift.User

case class DistinctUserFeaturesConfig(conf: Config) extends FeatureJobConfig[User] {
  val users          = HiveTextSource[User, Nothing](new Path("data/users"), Partitions.unpartitioned)
                       .distinctBy(_.id)

  val featureSource  = From[User]().bind(from(users))
  val featureSink    = HiveTextSink[Eavt]("userguide", new Path("dev/users"), "users", eavtByDay)
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
}

object DistinctUserFeaturesJob extends SimpleFeatureJob {
  def job = generate(DistinctUserFeaturesConfig(_), UserFeatures)
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

import commbank.coppersmith.util.Datestamp
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
    select(mdt => mdt._1.ageAt(Datestamp(mdt._2.getYear, mdt._2.getMonthOfYear, mdt._2.getDayOfMonth)))
      .asFeature(Continuous, "MOVIE_AGE", "Age of movie")

  val features = List(movieAgeFeature)
}
```

The context is passed through at the time of binding the concrete `DataSource`(s).

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.{DateTime, format}, format.DateTimeFormat

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
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

  val featureSink    = HiveTextSink[Eavt]("userguide", new Path("dev/movies"), "movies", eavtByDay)

  val featureContext = ExplicitGenerationTime(date)
}

object ContextFeaturesJob extends SimpleFeatureJob {
  val job = generate(ContextFeaturesConfig(_), ContextFeatures)
}


```


### Alternate sinks

If an output format different to `Eavt` is required, then a `Thrift`
struct defining the sink format is needed, as well as
an implicit implementation of `FeatureValueEnc` for the `Thrift` struct.
For example, this is a simple implementation where only the column names
are different.

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._, Coppersmith._
import commbank.coppersmith.examples.thrift.{FeatureEavt, User}

case class AlternativeSinkUserFeaturesConfig(conf: Config) extends FeatureJobConfig[User] {
  implicit object FeatureEavtEnc extends FeatureValueEnc[FeatureEavt] {
    def encode(fvt: (FeatureValue[Value], FeatureTime)): FeatureEavt = fvt match {
      case (fv, time) =>
        val featureValue = (fv.value match {
          case Integral(v)      => v.map(_.toString)
          case Decimal(v)       => v.map(_.toString)
          case FloatingPoint(v) => v.map(_.toString)
          case Str(v)           => v
          case Bool(v)          => v.map(_.toString)
          case Date(v)          => v.map(_.toString)
          case Time(v)          => v.map(_.toString)
        }).getOrElse(HiveTextSink.NullValue)

        val featureTime = new DateTime(time).toString("yyyy-MM-dd")
        FeatureEavt(fv.entity, fv.name, featureValue, featureTime)
    }
  }

  val partitions     = Partitions.unpartitioned
  val users          = HiveTextSource[User, Nothing](new Path("data/users"), partitions)

  val featureSource  = From[User]().bind(from(users))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val dbPrefix       = conf.getArgs("db-prefix")
  val dbRoot         = new Path(conf.getArgs("db-root"))
  val tableName      = conf.getArgs("table-name")

  val sinkPartition  = DerivedSinkPartition[FeatureEavt, (String, String, String)](
                         HivePartition.byDay(Fields[FeatureEavt].FeatureTime, "yyyy-MM-dd")
                       )
  val featureSink    = HiveTextSink[FeatureEavt](dbPrefix, dbRoot, tableName, sinkPartition)
}

object AlternativeSinkUserFeaturesJob extends SimpleFeatureJob {
  def job = generate(AlternativeSinkUserFeaturesConfig(_), UserFeatures)
}
```

Note: When using `HiveTextSink`, a `SinkPartition` is required.


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

import commbank.coppersmith.api._, scalding._, Coppersmith._, EavtText.{EavtEnc, eavtByDay}
import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.scalding.TypedPipeSource
import commbank.coppersmith.examples.thrift.{Movie, Rating}


case class Director(name: String, movieTitle: String)

object DirectorFeatures extends AggregationFeatureSet[(Director, Movie, Rating)] {
  val namespace                            = "userguide.examples"
  def entity(s: (Director, Movie, Rating)) = s._1.name

  val source = Join.multiway[Director]
    .inner[Movie].on((director: Director)     => director.movieTitle,
                     (movie: Movie)           => movie.title)
    .inner[Rating].on((d: Director, m: Movie) => m.id,
                      (rating: Rating)        => rating.movieId)

  val select = source.featureSetBuilder(namespace, entity)

  val directorAvgRating = select(avg(_._3.rating))
    .asFeature(Continuous, "DIRECTOR_AVG_RATING", Some(MinMaxRange(0.0, 5.0)),
               "Average rating of all movies the director has directed")

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

  val featureSource  = source.bind(joinMulti(directorsSource, movies, ratings))
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
  val featureSink    = HiveTextSink[Eavt]("userguide", new Path("dev/directors"), "directors", eavtByDay)
}

object DirectorFeaturesJob extends SimpleFeatureJob {
  def job = generate(DirectorFeaturesConfig(_), DirectorFeatures)
}
```

### Blank Features

Quite often, you may have an existing feature job implemented outside of
Coppersmith. Examples include Hive or Scalding jobs, or even jobs implemented
outside the Scala/Hadoop ecosystem. It is useful to describe the metadata of
the generated features even if they are not themselves generated in coppersmith.

For that, we introduce the concept of "blank" features: features which have
coppersmith metadata but whose runtime logic is elsewhere. Below is an example
of a Metadata set for blank features:

```scala
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift._

object BlankCustomerFeatureSet extends MetadataSet[Customer] {
    def name = "BlankCustomerFeatureSet"

    val customerLoyalty = Metadata[Customer, Integral](
      "userguide.examples",
      "cust_loyalty",
      "A measure of the customer's loyalty, provided by external system",
      Discrete
    )

  def metadata = List(customerLoyalty)
}
```

With blank features, the usual metadata artifacts are generated even though
the logic of the feature is not expressed in coppersmith.


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
in the Netflix challenge
([usage license](http://files.grouplens.org/datasets/movielens/ml-100k-README.txt)),
and the list of directors from IMDb
([copyright/license](http://www.imdb.com/Copyright)).

The following commands will download the necessary files to a directory called `data`:

```
mkdir -p data/ratings data/movies data/users data/directors
curl -O http://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip -j -d data/movies ml-100k.zip ml-100k/u.item
unzip -j -d data/ratings ml-100k.zip ml-100k/u.data
unzip -j -d data/user ml-100k.zip ml-100k/u.user
rm ml-100k.zip
curl ftp://ftp.fu-berlin.de/pub/misc/movies/database/directors.list.gz | gzcat > data/directors/directors.list
```

If the last command above gives an error such as "curl: (6) Could not resolve host: ftp.fu-berlin.de", then instead try:

```
ftp_proxy=$http_proxy curl ftp://ftp.fu-berlin.de/pub/misc/movies/database/directors.list.gz | gzcat > data/directors/directors.list
```

Then if you, like me, are running Hadoop on Vagrant (there are easy instructions on how to do that
here
https://blog.cloudera.com/blog/2014/06/how-to-install-a-virtual-apache-hadoop-cluster-with-vagrant-and-cloudera-manager/)
you would do the following (as the `vagrant` user):

```
hdfs dfs -put data
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
