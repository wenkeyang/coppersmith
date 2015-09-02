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

Coppersmith separates two key concerns of feature generation:
the formula or calculation itself,
and the process of executing this calculation
on some source data.


### The `Feature` class

An individual feature is represented by the `Feature[S, V]` class.
Its two type parameters are
the *source* (or input) type
and the *value* (or output) type.
You can think of it as a function from `S` to `V`:
- The source type is typically a thrift struct,
  describing the schema of a raw table
  or (more likely) an "Analytical Record".
- The value type is one of `Integral`, `Decimal`, or `Str`.

A feature must also define some metadata, including:
- a *namespace*,
- a feature *name*,
- and a feature *type* (`Categorial` or `Continuous`).

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
  FeatureMetadata(namespace   = "userguide.examples",
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

import commbank.coppersmith.{DataSource, From, HiveTextSource, HydroSink}
import commbank.coppersmith.{FeatureJobConfig, SimpleFeatureJob}
import commbank.coppersmith.FeatureSource.fromFS
import commbank.coppersmith.SourceBinder.from

import commbank.coppersmith.example.thrift.Customer

case class CustomerFeaturesConfig(conf: Config) extends FeatureJobConfig[Customer] {
  val partition     = HivePartition.byDay(Fields[Customer].EffectiveDate, "yyyy-MM-dd")
  val partitionPath = DataSource.PartitionPath(partition, ("2015", "08", "28"))
  val customers     = HiveTextSource(new Path("/data/customers"), partitionPath)
  val maestroConf   = MaestroConfig(conf, "features", "CUST", "birthdays")
  val dbRawPrefix   = conf.getArgs("db-raw-prefix")  // from command-line option

  val featureSource = From[Customer]().configure(from(customers))
  val featureSink   = HydroSink(HydroSink.config(maestroConf, dbRawPrefix))
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
for each `FeatureSet`.
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
We then treat `select` as a function,
using it to define our feature.
Finally, we call `asFeature` to provide the feature metadata,
returning a `Feature` object.

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{FeatureSet, Feature}
import Feature.Type._, Feature.Value._
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
the maximum and minimum balance per account,
per calendar year.
Notice that `time` is set to be the year,
and this defines both the window for aggregation
as well as the value for "T" in the EAVT output for hydro.

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{AggregationFeatureSet, Feature}
import Feature.Type._, Feature.Value._
import commbank.coppersmith.example.thrift.Account

object accountFeatures extends AggregationFeatureSet[Account] {
  val namespace            = "userguide.examples"
  def entity(acc: Account) = acc.id
  def time(acc: Account)   = DateTime.parse(acc.effectiveDate).getYear

  val source = From[Account]()
  val select = source.featureSetBuilder(namespace, entity(_), time(_))

  val minBalance = select(min(_.balance))
    .asFeature(Continuous, "ACC_ANNUAL_MIN_BALANCE", "Minimum balance for the calendar year")
  val maxBalance = select(max(_.balance))
    .asFeature(Continuous, "ACC_ANNUAL_MAX_BALANCE", "Maximum balance for the calendar year")

  val aggregationFeatures = List(minBalance, maxBalance)
}
```


### Joins

To do.

### Testing

Guidelines for unit testing are still forthcoming.
