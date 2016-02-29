Coppersmith User Guide
======================

This is a guide to using the Coppersmith library to define and generate features.
It is aimed at programmers and data engineers.
The [**Basics**](#basics) section introduces the fundamental concepts
and should be read in full.
The [**Intermediate**](#intermediate) section is also highly recommended.
The [**Advanced**](#advanced) section serves as a reference
to additional features of the library.
The [**Example**](#example) section contains a simple example of how Coppersmith
could be used to create a feature from the publicly available
[MovieLens](http://grouplens.org/datasets/movielens/) dataset.

In addition to this document, there is also a
[troubleshooting guide](TROUBLESHOOTING.markdown) available.


Basics
------


### Getting started

Add the `coppersmith-scalding` package to your SBT configuration.
For example (please replace with the latest version number):

    libraryDependencies += "au.com.cba.omnia" %% "coppersmith-scalding" % "0.4.1-20151129224909-484f4ea"


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

package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Customer

object CustomerBirthYear extends Feature[Customer, Integral](
  Metadata[Customer, Integral](namespace   = "userguide.examples",
                               name        = "CUST_BIRTHYEAR",
                               description = "Calendar year in which the customer was born",
                               featureType = Continuous)
) {
  def generate(cust: Customer) = Some(
    FeatureValue(entity = cust.id,
                 name   = "CUST_BIRTHYEAR",
                 value  = DateTime.parse(cust.dob).getYear)
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
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Customer

object CustomerFeatures extends BasicFeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id

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
- `TypedPipeSource`: see [Generating features from custom scalding code](#generating-features-from-custom-scalding-code)

and one sink type:

- `HydroSink`: a file in the format required for ingestion into Hydro
  (CBA's internal feature store, which is currently a text-encoded EAVT
  format, but will evolve to support future versions of Hydro as well)

Here is an example of a job which materialises the feature set
which we defined in the previous section:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import au.com.cba.omnia.maestro.api.{HivePartition, Maestro}
import Maestro.{DerivedDecode, Fields}

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._

import commbank.coppersmith.examples.thrift.Customer

case class CustomerFeaturesConfig(conf: Config) extends FeatureJobConfig[Customer] {
  type Partition    = (String, String, String) // (Year, Month, Day)

  val partition     = HivePartition.byDay(Fields[Customer].EffectiveDate, "yyyy-MM-dd")
  val partitions    = ScaldingDataSource.Partitions(partition, ("2015", "08", "28"))
  val customers     = HiveTextSource[Customer, Partition](new Path("/data/customers"), partitions)

  val featureSource = From[Customer]().bind(from(customers))

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 8, 29, 0, 0))

  val dbPrefix      = conf.getArgs("db-prefix")
  val dbRoot        = new Path(conf.getArgs("db-root"))
  val tableName     = conf.getArgs("table-name")

  val featureSink   = HydroSink.configure(dbPrefix, dbRoot, tableName)
}

object CustomerFeaturesJob extends SimpleFeatureJob {
  def job = generate(CustomerFeaturesConfig(_), CustomerFeatures)
}
```

Individual data sources that are common to different feature sources can be
pulled up to their own type for reuse, for example:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api.{HivePartition, Maestro}
import Maestro.{DerivedDecode, Fields}
import commbank.coppersmith.api.scalding._

import commbank.coppersmith.examples.thrift.Customer

case class CustomerSourceConfig(date: DateTime) {
  type Partition = (String, String, String) // (Year, Month, Day)

  def toPartitionVal(date: DateTime) =
    (date.getYear.toString, f"${date.getMonthOfYear}%02d", f"${date.getDayOfMonth}%02d")

  val partition    = HivePartition.byDay(Fields[Customer].EffectiveDate, "yyyy-MM-dd")
  val partitionVal = ScaldingDataSource.Partitions(partition, toPartitionVal(date))

  val dataSource   = HiveTextSource[Customer, Partition](new Path("/data/customers"), partitionVal)
}
```


### Partition selection

For partitioned tables, you can list more than one partition.
Additionally, you can use any glob/wildcard pattern that Hadoop supports.
For example:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api.{HivePartition, Maestro}
import Maestro.{DerivedDecode, Fields}
import commbank.coppersmith.api.scalding._

import commbank.coppersmith.examples.thrift.Customer

object MultiPartitionSnippet {
  type Partition = (String, String, String)

  // Last two days of July, and all of August
  val partition  = HivePartition.byDay(Fields[Customer].EffectiveDate, "yyyy-MM-dd")
  val partitions = ScaldingDataSource.Partitions(partition,
                     ("2015", "07", "30"), ("2015", "07", "31"), ("2015", "08", "*"))

  val customers  = HiveTextSource[Customer, Partition](new Path("/data/customers"), partitions)
}
```

For unpartitioned tables, use the following pattern:

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api.Maestro.DerivedDecode

import commbank.coppersmith.api.scalding._
import commbank.coppersmith.examples.thrift.Customer

object UnpartitionedSnippet {
  val customers = HiveTextSource[Customer, Nothing](new Path("/data/customers"),
                    ScaldingDataSource.Partitions.unpartitioned)
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
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._

import commbank.coppersmith.examples.thrift.Customer

object CustomerFeaturesFluent extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer, ctx: FeatureContext)   = DateTime.parse(cust.effectiveDate).getMillis


  val source = From[Customer]()  // FeatureSource (see above)
  val select = source.featureSetBuilder(namespace, entity)

  val customerBirthDay  = select(_.dob)
    .asFeature(Nominal, "CUST_BIRTHDAY", "Day on which the customer was born")
  val customerBirthYear = select(cust => DateTime.parse(cust.dob).getYear)
    .asFeature(Continuous, "CUST_BIRTHYEAR", "Calendar year in which the customer was born")

  val features = List(customerBirthDay, customerBirthYear)
}
```

Notice that in the above example, we overrode the `time` method to give us the
feature time based on the customer's effective date. This is possible, but most
of the time the default implementation, which returns the time specified by the
feature context is the correct thing to do.


Advanced
--------


### Tip: Extension methods for thrift structs

If you find yourself repeating certain calculations
(such as the date parsing in previous examples),
you may find that defining a "rich" version of the thrift struct
can help to keep feature definitions clear and concise.

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.{DateTime, Period}

import commbank.coppersmith.examples.thrift.{Customer, Account}

object Implicits {
  implicit class RichCustomer(cust: Customer) {
    def birthDate: DateTime = DateTime.parse(cust.dob)
    def birthYear: Int      = birthDate.getYear

    def ageAt(date: DateTime): Int = new Period(birthDate, date).getYears
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
package commbank.coppersmith.examples.userguide

import commbank.coppersmith.api._

import commbank.coppersmith.examples.thrift.Customer

import Implicits.RichCustomer

object Example {
  val customerPivotFeatures: PivotFeatureSet[Customer] =
    PivotMacro.pivotThrift[Customer]("userguide.examples", _.id)
}
```


### Aggregation (aka `GROUP BY`)

By subclassing `AggregationFeatureSet` and using `FeatureBuilder`,
you gain access to a number of useful aggregate functions:
`count`, `avg`, `max`, `min`, `sum`, and `uniqueCountBy`.
These are convenience methods for creating
[Algebird `Aggregator`s](https://github.com/twitter/scalding/wiki/Aggregation-using-Algebird-Aggregators).
Other aggregators can be defined by providing your own
`Aggregator` instance (see the `balanceRangeSize` feature in the example that
follows).

The grouping criteria (in SQL terms, the `GROUP BY` clause)
is implicitly *the entity and the time*,
as defined by the `entity` and `time` properties.

Note that when using `AggregationFeatureSet`,
you should *not* override `features`;
provide `aggregationFeatures` instead. Also, there is no
option to specify time as a function of the source record,
so the time will always come from the job's `FeatureContext`.

Here is an example that finds
the maximum and minimum end-of-day balance per account,
per calendar year
(based on the assumption that `Account` is updated
once per day at close of business)
Notice that `time` is set to be the year,
and this defines both the window for aggregation
as well as the value for "T" in the EAVT output for hydro.

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Account

import Implicits.RichAccount

object AccountFeatures extends AggregationFeatureSet[Account] {
  val namespace            = "userguide.examples"
  def entity(acc: Account) = acc.id

  val source = From[Account]()
  val select = source.featureSetBuilder(namespace, entity)

  val minBalance = select(min(_.balance))
    .asFeature(Continuous, "ACC_ANNUAL_MIN_BALANCE",
               "Minimum end-of-day balance for the calendar year")

  val maxBalance = select(max(_.balance))
    .asFeature(Continuous, "ACC_ANNUAL_MAX_BALANCE",
               "Maximum end-of-day-balance for the calendar year")

  // Somewhat contrived custom aggregator for the purposes of demonstration
  import com.twitter.algebird.Aggregator
  val rangeSize: Aggregator[Int, (Int, Int), Int] =
    Aggregator.min[Int].join(Aggregator.max[Int])
      .andThenPresent{ case (min, max) => max - min }

  val balanceRangeSize = select(rangeSize.composePrepare[Account](_.balance))
    .asFeature(Continuous, "BALANCE_RANGE_SIZE",
               "Size of the range between min and max balance")

  val aggregationFeatures = List(minBalance, maxBalance)
}
```

<a name="aggregator-source-view-note" />
Note that when using Aggregators with [source views](#source-views), the
`Aggregator` must be specified explicitly instead of using the inherited
aggregate functions. This is because the inherited functions are tied directly
to the `AggregationFeatureSet` source type. See the
[note](#source-view-aggregator-note) in the source views guide for an example
of this.


### Filtering (aka `WHERE`)

Features need not be defined for every input value.
When defining features using the fluent API,
one or more filters can be added using the `where` method
(`andWhere` is also a synonym,
to improve readability when there are multiple conditions).

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Customer

import Implicits.RichCustomer

object CustomerBirthFeatures extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id

  val source = From[Customer]()
  val select = source.featureSetBuilder(namespace, entity)

  val ageIn1970  = select(1970 - _.birthYear)
    .where(_.birthYear < 1970)
    .asFeature(Discrete, "CUST_AGE_1970",
               "Age in 1970, for customers born prior to 1970")

  val ageIn1980 = select(1980 - _.birthYear)
    .where   (_.birthYear >= 1970)
    .andWhere(_.birthYear <= 1979)
    .asFeature(Discrete, "CUST_AGE_1980",
               "Age in 1980, for customers born between 1970 and 1979")

  val features = List(ageIn1970, ageIn1980)
}
```

If a filter is common to all features from a single source,
the filter can be defined on the source itself to prevent
repetition in the feature definitions.

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Customer

import Implicits.RichCustomer

object GenXYCustomerFeatures extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id

  // Common filter applied to all features built from this source
  val source = From[Customer]().filter(c => Range(1960, 2000).contains(c.birthYear))
  val select = source.featureSetBuilder(namespace, entity)

  val genXBirthYear  = select(_.birthYear)
    .where(_.birthYear < 1980)
    .asFeature(Discrete, "GEN_X_CUST_BIRTH_YEAR",
               "Year of birth for customers born between 1960 and 1980")

  val genYBirthYear  = select(_.birthYear)
    .where(_.birthYear >= 1980)
    .asFeature(Ordinal, "GEN_Y_CUST_BIRTH_YEAR",
               "Year of birth for customers born between 1980 and 2000")

  val features = List(genXBirthYear, genYBirthYear)
}
```

In the special case where the value is always the same,
but the filter varies,
consider using the `QueryFeatureSet`:

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Customer

import Implicits.RichCustomer

object CustomerBirthFlags extends QueryFeatureSet[Customer, Str] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id

  def value(cust: Customer)  = "Y"
  val featureType            = Nominal

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
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.{Customer, Account}

import Implicits.RichCustomer

object JoinFeatures extends AggregationFeatureSet[(Customer, Account)] {
  val namespace                      = "userguide.examples"
  def entity(s: (Customer, Account)) = s._1.id

  val source = Join[Customer].to[Account].on(
    cust => cust.id,
    acc  => acc.customer
  )
  val select = source.featureSetBuilder(namespace, entity)

  val totalBalanceForCustomersBornPre1970 = select(sum(_._2.balance))
    .where(_._1.birthYear < 1970)
    .asFeature(Continuous, "CUST_BORN_PRE1970_TOT_BALANCE",
               "Total balance for customer born before 1970")

  val aggregationFeatures = List(totalBalanceForCustomersBornPre1970)
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

import au.com.cba.omnia.maestro.api.Maestro.DerivedDecode

import commbank.coppersmith.api._, scalding._
import commbank.coppersmith.examples.thrift.{Customer, Account}

case class JoinFeaturesConfig(conf: Config) extends FeatureJobConfig[(Customer, Account)] {
  val customers      = HiveTextSource[Customer, Nothing](new Path("data/customers"),
                         ScaldingDataSource.Partitions.unpartitioned)
  val accounts       = HiveTextSource[Account, Nothing](new Path("data/accounts"),
                         ScaldingDataSource.Partitions.unpartitioned)

  val featureSource  = JoinFeatures.source.bind(join(customers, accounts))
  val featureSink    = HydroSink.configure("dd", new Path("dev"), "balances")
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 8, 29, 0, 0))
}

object JoinFeaturesJob extends SimpleFeatureJob {
  def job = generate(JoinFeaturesConfig(_), JoinFeatures)
}
```


### Multiway joins

Joins between more than two tables are also possible, using the `multiway` function.

For example, (also contrived) the customer's total balance across all accounts
which have additional account holders:

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.{Customer, Account}

object JoinFeatures2 extends AggregationFeatureSet[(Customer, Account, Option[Customer])] {
  val namespace = "userguide.examples"

  def entity(s: (Customer, Account, Option[Customer])) = s._1.id

  val source = Join.multiway[Customer]
      .inner[Account].on((cust: Customer)             => cust.acct,
                         (acc: Account)               => acc.id)
      .left[Customer].on((c1: Customer, acc: Account) => acc.id,
                         (c2: Customer)               => c2.acct)
      .src  // Note the use of the .src call. Awkward implementation detail

  val select = source.featureSetBuilder(namespace, entity)

  // Make sure the other customer is defined and not us (in reality this would have
  // been an inner join but for the sake of education, we are showing the left)
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


### Source views

Sometimes it is convenient to work with a different view of
the source when defining features. This might simply be a
mapping to a different type, or a pattern-based filter and
extraction. This is akin to Scala's `map` and `collect`
methods on the standard collections, and the same methods
are available on the `FeatureSetBuilder`.

The following example only generates name features for
customers whose name is defined (ie, not `None`). Notice how
even though the `Customer.name` type is `Option[String]`,
there is no need to unwrap the option in the select clause
as it has already been extracted as part of matching against
`Some` in the `collect`.

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Customer

object SourceViewFeatures extends FeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id

  val source  = From[Customer]()
  val builder = source.featureSetBuilder(namespace, entity)

  val customerName =
    builder.map(c => (c, c.name)).collect { case (c, Some(name)) => (c, name) }
      .select(_._2)
      .asFeature(Nominal, "CUST_NAME_AVAILABLE", "Customer name when it is known")

  val features = List(customerName)
}
```

<a name="source-view-aggregator-note" />
As [noted](#aggregator-source-view-note) in the Aggregation features section,
when combining Aggregation features with source views, if the type of the source
view differs from the underlying feature set source type, the inherited
aggregator functions can no longer be used and the aggregator must be explicitly
specified.

```scala
package commbank.coppersmith.examples.userguide

import com.twitter.algebird.Aggregator

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Customer
import Implicits.RichCustomer

object SourceViewAggregationFeatures extends AggregationFeatureSet[Customer] {
  val namespace              = "userguide.examples"
  def entity(cust: Customer) = cust.id
  def time(cust: Customer)   = DateTime.parse(cust.effectiveDate).getMillis

  val source  = From[Customer]()
  val builder = source.featureSetBuilder(namespace, entity)

  val adultMinBalance =
    builder.collect { case c if Range(1960, 1980).contains(c.birthYear) => c.balance }
      .select(Aggregator.min[Int])
      .asFeature(Continuous, "GEN_X_MIN_BALANCE", "Minimum balance for gen-X customers")

  val aggregationFeatures = List(adultMinBalance)
}
```


### Generating values from job context

Sometimes it is necessary to use job specific data for generating feature
values, for example, using date information from the job configuration to
calculate the age of a person from their date of birth at the time of
generating a feature.

This can be achieved by incorporating a context with the `FeatureSet`'s source.

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.DateTime

import commbank.coppersmith.api._
import commbank.coppersmith.examples.thrift.Customer

import Implicits.RichCustomer
// The context, a DateTime in this case, forms part of the FeatureSource
object ContextFeatures extends FeatureSet[(Customer, DateTime)] {
  val namespace = "userguide.examples"

  def entity(s: (Customer, DateTime)) = s._1.id

  // Incorporate context with FeatureSource as a type
  val source = From[Customer].withContext[DateTime]

  val select = source.featureSetBuilder(namespace, entity)

  def customerAgeFeature =
    select(cdt => cdt._1.ageAt(cdt._2))
      .asFeature(Ordinal, "CUST_AGE", "Age of customer")

  val features = List(customerAgeFeature)
}
```

The context is passed through at the time of binding the concrete `DataSource`(s).

```scala
package commbank.coppersmith.examples.userguide

import org.joda.time.{DateTime, format}, format.DateTimeFormat

import com.twitter.scalding.Config

import commbank.coppersmith.api._, scalding._

import commbank.coppersmith.examples.thrift.Customer

abstract class ContextFeaturesConfig(conf: Config)
    extends FeatureJobConfig[(Customer, DateTime)] {

  val date = conf.getArgs.optional("date").map(d =>
               DateTime.parse(d, DateTimeFormat.forPattern("yyyy-MM-dd"))
             ).getOrElse(new DateTime().minusDays(1))

  val customers = CustomerSourceConfig(date).dataSource

  // Note: Current date passed through as context param
  val featureSource =
    ContextFeatures.source.bindWithContext(from(customers), date)
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
(such as `customerIds` in the following example)
you can bind it to the feature source.
An SQL analogy would be using a view, in place of a concrete table.

```scala
package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config
import com.twitter.scalding.typed.TypedPipe

import org.joda.time.DateTime

import commbank.coppersmith.api._, scalding._

object MillionthCustomerFeatures extends FeatureSetWithTime[String] {
  val namespace           = "userguide.examples"
  def entity(cid: String) = cid

  val source = From[String]
  val select = source.featureSetBuilder(namespace, entity)

  val customerMillionth = select(_ => "Y")
    .asFeature(Nominal, "CUST_MILLIONTH", "Y if customer id is divisible by 1,000,000")

  val features = List(customerMillionth)
}

case class MillionthCustomerFeaturesConfig(conf: Config) extends FeatureJobConfig[String] {
  // FIXME: replace with a more compelling example, after we switch our examples
  // over to a new (non-banking) domain.
  val step           = 1000000
  val max            = step * 10
  val customerIds    = TypedPipe.from(step to max by step).map(_.toString)

  val featureSource  = MillionthCustomerFeatures.source.bind(from(TypedPipeSource(customerIds)))
  val featureSink    = HydroSink.configure("dd", new Path("dev"), "customers")
  val featureContext = ExplicitGenerationTime(new DateTime(2015, 8, 29, 0, 0))
}

object MillionthCustomerFeaturesJob extends SimpleFeatureJob {
  def job = generate(MillionthCustomerFeaturesConfig(_), MillionthCustomerFeatures)
}
```


### Testing

Guidelines for unit testing are still forthcoming.

Example
-------

This is an example of using Coppersmith with a publicly available data set in order to demonstrate
its functionality and make the objectives of the project clearer. There is a lot more that can be
done in Coppersmith but this is intended as a quick "Hello Feature" job.

### Setup

In order to run the example you first need to obtain the famous Movie Lens data set that was used
in the Netflix challenge.

http://grouplens.org/datasets/movielens/

unzip the ml-100k.zip in your project directory under data/ml-100k/

Then if you, like me, are running Hadoop on Vagrant (there are easy instructions on how to do that
here
https://blog.cloudera.com/blog/2014/06/how-to-install-a-virtual-apache-hadoop-cluster-with-vagrant-and-cloudera-manager/)
you would do something similar to:

As user hdfs in my case:
```
hdfs dfs -mkdir -p /data/rating
hdfs dfs -mkdir /data/movie
hdfs dfs -chmod -R a+rwx /data
```

As user vagrant:
```
hdfs dfs -copyFromLocal /vagrant/data/ml-100k/u.item /data/movie/
hdfs dfs -copyFromLocal /vagrant/data/ml-100k/u.data /data/rating/
```


In my case as this is a fresh cluster I had to create the home directory for the vagrant user
(as user hdfs):
```
hdfs dfs -mkdir /user/vagrant
hdfs dfs -chown vagrant /user/vagrant
```

### Defining the feature

In this case we want to obtain the average movie rating. In order to achieve that we need to join
the u.data table that contains the actual ratings, with the u.item ratings that contains the movie titles.

The first step in achieving that is creating the thrift structure where the fields are contained.
In this case two are needed that can be found in:
[Movie.thrift](examples/src/main/thrift/Movie.thrift)
and
[Rating.thrift](examples/src/main/thrift/Rating.thrift)

Finally the method of generating the required aggregation and join feature and the job setup need to be defined:
[MovieLens.scala](examples/src/main/scala/commbank/coppersmith/examples/MovieLens.scala)

### Running the example

In order to run the example you will need to do something like:
```
hadoop jar /vagrant/examples/target/scala-2.11/coppersmith-examples-assembly-0.5.3-20160114033303-c83b6ab-SNAPSHOT.jar commbank.coppersmith.examples.userguide.MovieFeaturesJob -hdfs
```

You can then inspect the output on hdfs under /data/output where the problem with the data set becomes obvious:
```
Star Trek: Generations (1994)|AVERAGE_MOVIE_RATING|3.336206896551725
```
It should be 5 as there can't possibly any other rating for that movie.
