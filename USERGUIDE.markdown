Coppersmith User Guide
======================

This is a guide to using the Coppersmith library to generate features.
It is aimed at programmers and data engineers.
The **Basics** section introduces the fundamental concepts
and should be read in full.
The **Intermediate** section is also highly recommended.
The **Advanced** section serves as a reference
to additional features of the library.


Basics
------


### Recommended project structure

Coppersmith separates two key concerns of feature generation:
the formula or calculation itself (feature definition),
and the process of executing this calculation
on some source data (feature job).

Feature definition requires the `coppersmith-core` package,
and has no dependencies on any particular execution framework,
e.g. scalding.
To turn the feature definition into a Hadoop job
requires the `coppersmith-scalding` package.
Other execution frameworks may be supported in the future.

There should be one top-level repo for each *customer*
(e.g. CEP, CommScore),
and it is recommended that the repo be divided into subprojects
as follows:

- **_customer_-features**: feature definitions
  (depends on `coppersmith-core`)
- **_customer_-scaffolding**: feature jobs
  (depends on `coppersmith-scalding`)
- **_customer_-auxiliary**: Autosys JILs
  (no dependencies)


### The `Feature` class

An individual feature is represented by the `Feature[S, V]` class.
The two type parameters are
the *source* (or input) type
and the *value* (or output) type.
You can think of it as a function from `S` to `V`:
- The source type is typically a thrift struct,
  describing the schema of a raw table
  or (more likely) an "Analytical Record".
- The value type is one of `Integral`, `Decimal`, or `Str`.

A feature must also define some metadata, including:
- a feature *namespace*,
- a feature *name*, and
- a feature *type* (`Categorial` or `Continuous`).

Below is an example of a feature defined by extending the `Feature` class.
If this looks complicated, don't worry!
In the next section,
we'll see how this can be made a lot easier.

```scala
// NOTE: This example is for pedagogical purposes only; it is not the
// recommended approach. Consider using the convenience methods of a
// FeatureSet subclass or the featureBuilder API (see below for both).

import org.joda.time.DateTime

import commbank.coppersmith.{Feature, FeatureMetadata, FeatureValue}
import Feature.Type._, Feature.Value._
import commbank.coppersmith.example.thrift.Customer

object customerBirthYear extends Feature[Customer, Integral](
  FeatureMetadata[Integral](namespace   = "userguide.examples",
                            name        = "CUST_BIRTHYEAR",
                            description = "Calendar year in which the customer was born",
                            featureType = Continuous)
) {
  def generate(cust: Customer) = Some(
    FeatureValue(entity = cust.id,
                 name   = "CUST_BIRTHYEAR",
                 value  = DateTime.parse(cust.dob).getYear,
                 time   = DateTime.parse(cust.effectiveDate).getMillis)
  )
}
```


### The `FeatureSet` class

`FeatureSet[S]` is a group of features
derived from the same source record type `S`.
There are several specialised subtypes
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
import org.joda.time.DateTime

import commbank.coppersmith.{BasicFeatureSet, Feature}
import Feature.Type._, Feature.Value._
import commbank.coppersmith.example.thrift.Customer

object customerFeatures extends BasicFeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer)   = DateTime.parse(cust.effectiveDate).getMillis

  val customerBirthYear = basicFeature[Integral](
    "CUST_BIRTHYEAR", "Calendar year in which the customer was born", Continuous,
    (cust) => DateTime.parse(cust.dob).getYear
  )

  val features = List(customerBirthYear)
}
```


### Execution: the `SimpleFeatureJob` class

Coppersmith is part of a broader programme of work
that aims to simplify all the repetitive aspects of feature generation,
including deployment and execution.
However, we are not there yet!
For the moment,
you still need to create a mainline scalding job
to execute the features which you have defined,
and deploy it in the usual way
with `ops.logical` and Autosys.

To make this as easy as possible,
Coppersmith provides a class called `SimpleFeatureJob`,
which helps turn a `FeatureSet` into a maestro job.
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

- `HiveTextSource`: a delimited file with Hive conventions
- `HiveParquetSource`: a parquet-encoded file

and one sink type:

- `HydroSink`: a file in the format required for ingestion into Hydro
  (this is currently a text-encoded EAVT format, but will evolve to support
  future versions of Hydro as well)

Here is an example of a job which materialises the feature set
which we defined in the previous section:

```scala
import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import au.com.cba.omnia.maestro.api.{MaestroConfig, HivePartition, Maestro}
import Maestro._

import commbank.coppersmith.From
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.SourceBinder.from

import commbank.coppersmith.example.thrift.Customer

// imports from the coppersmith-scalding package
import commbank.coppersmith.scalding.{FeatureJobConfig, SimpleFeatureJob}
import commbank.coppersmith.scalding.{ScaldingDataSource, HiveTextSource, HydroSink}
import commbank.coppersmith.scalding.framework

case class CustomerFeaturesConfig(conf: Config) extends FeatureJobConfig[Customer] {
  val partition     = HivePartition.byDay(Fields[Customer].EffectiveDate, "yyyy-MM-dd")
  val partitionPath = ScaldingDataSource.PartitionPath(partition, ("2015", "08", "28"))
  val customers     = HiveTextSource(new Path("/data/customers"), partitionPath)
  val maestroConf   = MaestroConfig(conf, "features", "CUST", "birthdays")
  val dbRawPrefix   = conf.getArgs("db-raw-prefix")  // from command-line option

  val featureSource = From[Customer]().bind(from(customers))
  val featureSink   = HydroSink.configure(maestroConf, dbRawPrefix)
}

object CustomerFeaturesJob extends SimpleFeatureJob {
  def job = generate(CustomerFeaturesConfig(_), customerFeatures)
}
```


Intermediate
------------

This section introduces an alternative API for feature definitions.


### Shaping input data: the `FeatureSource`

In the example above,
we quietly snuck in `From[Customer]()` without explanation.
The type of this expression
(or rather, the implicit type, after conversion)
is `FeatureSource`.
This is a trivial example,
since it simply indicates that
the input source is a stream of `Customer` records.
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
import org.joda.time.DateTime

import commbank.coppersmith.{FeatureSet, Feature}
import Feature.Type._, Feature.Value._
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Customer

object customerFeaturesFluent extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer)   = DateTime.parse(cust.effectiveDate).getMillis

  val source = From[Customer]()  // FeatureSource (see above)
  val select = source.featureSetBuilder(namespace, entity(_), time(_))

  val customerBirthDay  = select(_.dob)
    .asFeature(Categorical, "CUST_BIRTHDAY", "Day on which the customer was born")
  val customerBirthYear = select(cust => DateTime.parse(cust.dob).getYear)
    .asFeature(Continuous, "CUST_BIRTHYEAR", "Calendar year in which the customer was born")

  val features = List(customerBirthDay, customerBirthYear)
}
```


Advanced
--------


### Tip: Extension methods for thrift structs

If you find yourself repeating certain calculations
(such as the date parsing in previous examples),
you may find that defining a "rich" version of the thrift struct
can help to keep feature definitions clear and concise.

```scala
import commbank.coppersmith.example.thrift.{Customer, Account}

object Implicits {
  implicit class RichCustomer(cust: Customer) {
    def timestamp: Long = DateTime.parse(cust.effectiveDate).getMillis
    def birthYear: Int  = DateTime.parse(cust.dob).getYear
  }

  implicit class RichAccount(acc: Account) {
    def eventYear: Int = DateTime.parse(acc.effectiveDate).getYear
  }
}
```

Subsequent examples will use concise syntax
such as `_.birthYear` wherever possible.


### Aggregation (aka GROUP BY)

By subclassing `AggregationFeatureSet`,
you gain access to a number of useful aggregate functions:
`count`, `avg`, `max`, `min`, `sum`, and `uniqueCountBy`.
(If you need something that isn't listed here,
please raise an issue).

The grouping criteria (in SQL terms, the GROUP BY clause)
is implicitly *the entity and the time*,
as defined by the `entity` and `time` properties.

Note that when using `AggregationFeatureSet`,
you should *not* override `features`;
provide `aggregationFeatures` instead.

Here is an example that finds
the maximum and minimum end-of-day balance per account,
per calendar year
(based on the assumption that `Account` is updated
once per day at close of business)
Notice that `time` is set to be the year,
and this defines both the window for aggregation
as well as the value for "T" in the EAVT output for hydro.

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{AggregationFeatureSet, Feature}
import Feature.Type._, Feature.Value._
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Account

import Implicits.RichAccount

object accountFeatures extends AggregationFeatureSet[Account] {
  val namespace            = "userguide.examples"
  def entity(acc: Account) = acc.id
  def time(acc: Account)   = acc.eventYear

  val source = From[Account]()
  val select = source.featureSetBuilder(namespace, entity(_), time(_))

  val minBalance = select(min(_.balance))
    .asFeature(Continuous, "ACC_ANNUAL_MIN_BALANCE",
               "Minimum end-of-day balance for the calendar year")

  val maxBalance = select(max(_.balance))
    .asFeature(Continuous, "ACC_ANNUAL_MAX_BALANCE",
               "Maximum end-of-day-balance for the calendar year")

  val aggregationFeatures = List(minBalance, maxBalance)
}
```


### Filtering (aka WHERE)

Features need not be defined for every input value.
When defining features using the fluent API,
one or more filters can be added using the `where` method
(`andWhere` is also a synonym,
to improve readability when there are multiple conditions).

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{FeatureSet, Feature}
import Feature.Type._, Feature.Value._
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Customer

import Implicits.RichCustomer

object customerBirthFeatures extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer)   = cust.timestamp

  val source = From[Customer]()
  val select = source.featureSetBuilder(namespace, entity(_), time(_))

  val ageIn1970  = select(1970 - _.birthYear)
    .where(_.birthYear < 1970)
    .asFeature(Continuous, "CUST_AGE_1970",
               "Age in 1970, for customers born prior to 1970")

  val ageIn1980 = select(1980 - _.birthYear)
    .where   (_.birthYear >= 1970)
    .andWhere(_.birthYear <= 1979)
    .asFeature(Continuous, "CUST_AGE_1980",
               "Age in 1980, for customers born between 1970 and 1979")

  val features = List(ageIn1970, ageIn1980)
}
```

In the special case where the value is always the same,
but the filter varies,
consider using the `QueryFeatureSet`:

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{QueryFeatureSet, Feature}
import Feature.Type._, Feature.Value._
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Customer

import Implicits.RichCustomer

object customerBirthFlags extends QueryFeatureSet[Customer, Str] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer)   = cust.timestamp

  def value(cust: Customer)  = "Y"
  val featureType            = Categorical

  val source = From[Customer]()

  val bornPre1970 = queryFeature("CUST_BORN_PRE1970", "'Y' if born before 1970", _.birthYear < 1970)
  val bornPre1980 = queryFeature("CUST_BORN_PRE1980", "'Y' if born before 1980", _.birthYear < 1980)

  val features = List(bornPre1970, bornPre1980)
}
```


### Joins

At the type level,
a feature calculated from two joined tables
has a pair of thrift structs as its source,
e.g. `(Customer, Account)`.
For a left join, the value on the right may be missing,
e.g. `(Customer, Option[Account])`.

As described in the section **Shaping input data**,
the convention of defining a `source` per feature set
allows you to specify further detail about the join:

- Use `Join[A].to[B]` for an inner join
- Use `Join.left[A].to[B]` for a left outer join
- Use `.on(left: A => J, right: A => J)` to specify the join columns

An example (somewhat contrived)
might be the total balance across all accounts
for customers born before 1970:

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{AggregationFeatureSet, Feature, Join}
import Feature.Type._, Feature.Value._
import commbank.coppersmith.example.thrift.Account

object joinFeatures extends AggregationFeatureSet[(Customer, Account)] {
  val namespace                      = "userguide.examples"
  def entity(s: (Customer, Account)) = s._1.id
  def time(s: (Customer, Account))   = s._1.timestamp

  val source = Join[Customer].to[Account].on(
    cust => cust.id,
    acc  => acc.customer
  )
  val select = source.featureSetBuilder(namespace, entity(_), time(_))

  val totalBalanceForCustomersBornPre1970 = select(sum(_._2.balance))
    .where(_._1.birthYear < 1970)
    .asFeature(Continuous, "CUST_BORN_PRE1970_TOT_BALANCE",
               "Total balance for customer born before 1970")

  val aggregationFeatures = List(totalBalanceForCustomersBornPre1970)
}
```


### Testing

Guidelines for unit testing are still forthcoming.
