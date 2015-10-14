Coppersmith User Guide
======================

This is a guide to using the Coppersmith library to define and generate features.
It is aimed at programmers and data engineers.
The [**Basics**](#basics) section introduces the fundamental concepts
and should be read in full.
The [**Intermediate**](#intermediate) section is also highly recommended.
The [**Advanced**](#advanced) section serves as a reference
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

Thusly, feature code can be separated into two projects, such
that feature definitions can be maintained independently of the
framework code that is use to generate the feature values.

An example of how these two projects would be structured is as follows:

- **_my-org_-features**: feature definitions
  (depends on `coppersmith-core`)
- **_my-org_-scaffolding**: feature jobs
  (depends on `my-org-features` and `coppersmith-scalding`)


### The `Feature` class

An individual feature is represented by the `Feature[S, V]` type.
The two type parameters, `S` and `V`, are the *source* (or input)
type and the *value* (or output) type respectively.
You can think of it as a function from `S` to `V`:
- The source type is typically a thrift struct,
  describing the schema of a raw table
  or (more likely) an *Analytical Record*.
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

import commbank.coppersmith.{Feature, FeatureValue, FeatureContext}
import Feature.Metadata, Feature.Type._, Feature.Value._
import commbank.coppersmith.example.thrift.Customer

object CustomerBirthYear extends Feature[Customer, Integral](
  Metadata[Customer, Integral](namespace   = "userguide.examples",
                               name        = "CUST_BIRTHYEAR",
                               description = "Calendar year in which the customer was born",
                               featureType = Continuous)
) {
  def generate(cust: Customer, ctx: FeatureContext) = Some(
    FeatureValue(entity = cust.id,
                 name   = "CUST_BIRTHYEAR",
                 value  = DateTime.parse(cust.dob).getYear,
                 time   = ctx.generationTime.getMillis)
  )
}
```


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
import org.joda.time.DateTime

import commbank.coppersmith.{BasicFeatureSet, Feature}
import Feature.Type.Continuous, Feature.Value.Integral
import commbank.coppersmith.example.thrift.Customer

object CustomerFeatures extends BasicFeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer, ctx: FeatureContext)   = DateTime.parse(cust.effectiveDate).getMillis

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

and one sink type:

- `HydroSink`: a file in the format required for ingestion into Hydro
  (CBA's internal feature store, which is currently a text-encoded EAVT
  format, but will evolve to support future versions of Hydro as well)

Here is an example of a job which materialises the feature set
which we defined in the previous section:

```scala
import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import au.com.cba.omnia.maestro.api.{MaestroConfig, HivePartition, Maestro}
import Maestro.{DerivedDecode, Fields}

import commbank.coppersmith.{From, ParameterisedFeatureContext}
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.SourceBinder.from

import commbank.coppersmith.example.thrift.Customer

// imports from the coppersmith-scalding package
import commbank.coppersmith.scalding.{FeatureJobConfig, SimpleFeatureJob}
import commbank.coppersmith.scalding.{ScaldingDataSource, HiveTextSource, HydroSink}
import commbank.coppersmith.scalding.framework

case class CustomerFeaturesConfig(conf: Config) extends FeatureJobConfig[Customer] {
  type Partition    = (String, String, String) // (Year, Month, Day)

  val partition     = HivePartition.byDay(Fields[Customer].EffectiveDate, "yyyy-MM-dd")
  val partitions    = ScaldingDataSource.Partitions(partition, ("2015", "08", "28"))
  val customers     = HiveTextSource[Customer, Partition](new Path("/data/customers"), partitions)
  val maestroConf   = MaestroConfig(conf, "features", "CUST", "birthdays")
  val dbPrefix      = conf.getArgs("db-prefix")  // from command-line option

  val featureSource = From[Customer]().bind(from(customers))
  val featureSink   = HydroSink.configure(maestroConf, dbPrefix)
  
  val featureContext = ParameterisedFeatureContext(new DateTime(2015, 8, 29, 0, 0))
}

object CustomerFeaturesJob extends SimpleFeatureJob {
  def job = generate(CustomerFeaturesConfig(_), CustomerFeatures)
}
```


Intermediate
------------

This section introduces an alternative API for feature definitions.


### Shaping input data: the `FeatureSource`

In the example above,
we quietly snuck in `From[Customer]()` without explanation.
The type of this expression is `FeatureSource`.
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
import Feature.Type.{Categorical, Continuous}
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Customer

object CustomerFeaturesFluent extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer, ctx: FeatureContext)   = DateTime.parse(cust.effectiveDate).getMillis

  val source = From[Customer]()  // FeatureSource (see above)
  val select = source.featureSetBuilder(namespace, entity, time)

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


### Pivoting

The simplest kind of feature is simply the value of a field.
This pattern is known as "pivoting",
because it transforms a wide input record
into the narrow EAVT format.

The `pivotThrift` macro can be used to
easily create a feature set containing all the input fields
as separate features.

```scala
import commbank.coppersmith.{PivotMacro, PivotFeatureSet}

import commbank.coppersmith.example.thrift.Customer

import Implicits.RichCustomer

object Example {
  val customerPivotFeatures: PivotFeatureSet[Customer] =
    PivotMacro.pivotThrift[Customer]("userguide.examples", _.id, (c, ctx) => c.timestamp)
}
```


### Aggregation (aka `GROUP BY`)

By subclassing `AggregationFeatureSet`,
you gain access to a number of useful aggregate functions:
`count`, `avg`, `max`, `min`, `sum`, and `uniqueCountBy`.
These are convenience methods for creating
[Algebird `Aggregator`s](https://github.com/twitter/scalding/wiki/Aggregation-using-Algebird-Aggregators).
Other aggregators can be defined by providing your own
`Aggregator` instance.

The grouping criteria (in SQL terms, the `GROUP BY` clause)
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
import Feature.Type.Continuous
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Account

import Implicits.RichAccount

object AccountFeatures extends AggregationFeatureSet[Account] {
  val namespace            = "userguide.examples"
  def entity(acc: Account) = acc.id
  def time(acc: Account, ctx: FeatureContext)   = acc.eventYear

  val source = From[Account]()
  val select = source.featureSetBuilder(namespace, entity, time)

  val minBalance = select(min(_.balance))
    .asFeature(Continuous, "ACC_ANNUAL_MIN_BALANCE",
               "Minimum end-of-day balance for the calendar year")

  val maxBalance = select(max(_.balance))
    .asFeature(Continuous, "ACC_ANNUAL_MAX_BALANCE",
               "Maximum end-of-day-balance for the calendar year")

  val aggregationFeatures = List(minBalance, maxBalance)
}
```


### Filtering (aka `WHERE`)

Features need not be defined for every input value.
When defining features using the fluent API,
one or more filters can be added using the `where` method
(`andWhere` is also a synonym,
to improve readability when there are multiple conditions).

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{FeatureSet, Feature}
import Feature.Type.Categorical
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Customer

import Implicits.RichCustomer

object CustomerBirthFeatures extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer, ctx: FeatureContext)   = cust.timestamp

  val source = From[Customer]()
  val select = source.featureSetBuilder(namespace, entity, time)

  val ageIn1970  = select(1970 - _.birthYear)
    .where(_.birthYear < 1970)
    .asFeature(Categorical, "CUST_AGE_1970",
               "Age in 1970, for customers born prior to 1970")

  val ageIn1980 = select(1980 - _.birthYear)
    .where   (_.birthYear >= 1970)
    .andWhere(_.birthYear <= 1979)
    .asFeature(Categorical, "CUST_AGE_1980",
               "Age in 1980, for customers born between 1970 and 1979")

  val features = List(ageIn1970, ageIn1980)
}
```

If a filter is common to all features from a single source,
the filter can be defined on the source itself to prevent
repetition in the feature definitions.

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{FeatureSet, Feature}
import Feature.Type.Categorical
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Customer

import Implicits.RichCustomer

object GenXYCustomerFeatures extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer, ctx: FeatureContext)   = cust.timestamp

  // Common filter applied to all features built from this source
  val source = From[Customer]().filter(c => Range(1960, 2000).contains(c.birthYear))
  val select = source.featureSetBuilder(namespace, entity, time)

  val genXBirthYear  = select(_.birthYear)
    .where(_.birthYear < 1980)
    .asFeature(Categorical, "GEN_X_CUST_BIRTH_YEAR",
               "Year of birth for customers born between 1960 and 1980")

  val genYBirthYear  = select(_.birthYear)
    .where(_.birthYear >= 1980)
    .asFeature(Categorical, "GEN_Y_CUST_BIRTH_YEAR",
               "Year of birth for customers born between 1980 and 2000")

  val features = List(genXBirthYear, genYBirthYear)
}
```

In the special case where the value is always the same,
but the filter varies,
consider using the `QueryFeatureSet`:

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{QueryFeatureSet, Feature}
import Feature.Type.Categorical, Feature.Value.Str
import commbank.coppersmith.FeatureBuilderSource.fromFS
import commbank.coppersmith.example.thrift.Customer

import Implicits.RichCustomer

object CustomerBirthFlags extends QueryFeatureSet[Customer, Str] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer, ctx: FeatureContext)   = cust.timestamp

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

Self-joins are possible, yielding `(Customer, Customer)` for an inner self join,
and `(Customer, Option[Customer])` for an left outer self join.

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
import Feature.Type.Continuous
import commbank.coppersmith.example.thrift.Account

object JoinFeatures extends AggregationFeatureSet[(Customer, Account)] {
  val namespace                      = "userguide.examples"
  def entity(s: (Customer, Account)) = s._1.id
  def time(s: (Customer, Account), ctx: FeatureContext) = s._1.timestamp

  val source = Join[Customer].to[Account].on(
    cust => cust.id,
    acc  => acc.customer
  )
  val select = source.featureSetBuilder(namespace, entity, time)

  val totalBalanceForCustomersBornPre1970 = select(sum(_._2.balance))
    .where(_._1.birthYear < 1970)
    .asFeature(Continuous, "CUST_BORN_PRE1970_TOT_BALANCE",
               "Total balance for customer born before 1970")

  val aggregationFeatures = List(totalBalanceForCustomersBornPre1970)
}
```

### Multiway joins

Joins between more than two tables are also possible, using the `multiway` function.

For example, (also contrived) the customer's total balance across all accounts 
which have additional account holders:

```scala
import org.joda.time.DateTime

import commbank.coppersmith.{AggregationFeatureSet, Feature, Join}
import Feature.Type.Continuous
import commbank.coppersmith.example.thrift.Account


object JoinFeatures2 extends AggregationFeatureSet[(Customer, Account, Option[Customer])] {
  val namespace = "userguide.examples"
  
  def entity(s: (Customer, Account, Option[Customer])) = s._1.id
  def time  (s: (Customer, Account, Option[Customer]), ctx: FeatureContext) = s._1.timestamp

  val source = Join.multiway[Customer]
      .inner[Account].on((cust: Customer)             => cust.acct,
                         (acc: Account)               => acc.id)
      .left[Customer].on((c1: Customer, acc: Account) => acc.id,
                         (c2: Customer)               => c2.acct)
      .src  //Note the use of the .src call. Awkward implementation detail
  
  val select = source.featureSetBuilder(namespace, entity, time)

  //make sure the other customer is defined and not us (in reality this would have 
  //been an inner join but for the sake of education, we are showing the left)
  val totalBalanceForCustomersWithJointAccounts = select(sum(_._2.balance))
    .where(row => row._3.exists(_ != row._1.id))  
    .asFeature(Continuous, "CUST_JOINT_TOT_BALANCE",
               "Total balance for customer with joint account")

  val aggregationFeatures = List(totalBalanceForCustomersWithJointAccounts)
}

```

Notice that for multiway joins, we need to hint the types of the join functions
to the compiler. Each stage of the join needs two functions: one producing
a join column for all of the left values so far, and one for the current right 
value.

### Testing

Guidelines for unit testing are still forthcoming.
