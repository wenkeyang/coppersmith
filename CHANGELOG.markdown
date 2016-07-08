Change log
==========

## 0.18.0
Range metadata arguments added to Feature building classes, allowing ranges
to be easily specified in Feature Sets.

### Upgrading
 - Calls to `Patterns.general` and `Patterns.pivot` must include an
   `Option[Range[V]]` as the last argument
 - Calls to `SetRange(List[V])` must be changed to `SetRange(V*)`
 - Change
   `QueryFeature(featureName, humanDescriptionfilter)`
   to
   `QueryFeature(featureName, humanDescription)(filter)`
   and
   `basicFeature[V <: Value : TypeTag](featureName, humanDescription, featureType, value)`
   to
   `basicFeature[V <: Value : TypeTag](featureName, humanDescription, featureType)(value)`

## 0.17.0
First-class support for features with boolean outputs via the new `Feature.Value.Bool` type.

## 0.16.0
Shapeless multi-way join support replaced with generated code solution
for more consistency in defining and binding feature sources. Also
allows multi-way joins to be bound with a context (this was not
previously possible for multi-way joins).

### Upgrading
 - Remove trailing `.src` call from joined source declaration, eg, remove the
   last line from the following:
```scala
  val source = Join.multiway[Movie]
      .inner[Rating].on((movie: Movie)               => movie.id,
                         (rating: Rating)            => rating.movieId)
      .inner[User].on((movie: Movie, rating: Rating) => rating.userId,
                         (user: User)                => user.id)
      .src
```
 - Remove the source parameter from the `bind` call and flatten
   `joinMulti` tuple, eg, change:
```scala
   MultiJoinFeatures.source.bind(joinMulti((movies, ratings, users), MultiJoinFeatures.source))
```
   to
```scala
   MultiJoinFeatures.source.bind(joinMulti(movies, ratings, users))
```

## 0.15.0
Introduction of `Date` and `Time` value types and `Instant` feature type.

### Upgrading
 - References to the `Feature.Time` alias should be replaced with
   `Feature.FeatureTime`

## 0.14.0
Allow features values and aggregations to use `BigDecimal`

- Change the value type of `Feature.Value.Decimal` to `BigDecimal`
- Add `Feature.Value.FloatingPoint` that has a value of `Double`
- Allow aggregations to accept and return `BigDecimal`
- Add new aggregator `avgBigDec` to allow `BigDecimal` aggregations

### Upgrading

 - References to `Decimal` must either be passed a `BigDecimal`, or be changed to `FloatingPoint`

## 0.13.0
Remove `TextSink.Configure`. This removes the hardcoded path
(`/$dbRoot/view/warehouse/features/$groups/$tableName`), and the hardcoded
database name (`"$dbPrefix_features"`).

- Removed `TextSink.Config`, `TextSink.configure`, `EavtTextSink.Config` and `EavtTextSink.configure`
- Renamed `EavtTextSink` to `EavtText`
- Changed `TextSink[T](conf: TextSink.Config)` to
 `HiveTextSink[T](
   dbName:    TextSink.DatabaseName,
   tablePath: Path,
   tableName: TextSink.TableName,
   partition: SinkPartition[T],
   delimiter: String = TextSink.Delimiter,
   dcs:       DelimiterConflictStrategy[T] = FailJob[T]()
 )`

 ### Upgrading

 - References to `TextSink(Config)` must be changed to the new signature.
 - References to `TextSink.configure(...)` should be changed to `HiveTextSink(...)`
 - References to `EavtTextSink.configure(...)` should be changed to
  `HiveTextSink[Eavt](...)`, with `EavtText.eavtByDay` passed as the partition arg,
  and `EavtText.EavtEnc` in scope
 - References to `EavtTextSink.defaultPartition` must be changed to
  `EavtText.eavtByDay`
 - Ensure that the path and database name passed to `HiveTextSink` are correct
  (including `.../view/warehouse/features/...` and `"..._features"`)
 - Refer to [USERGUIDE](USERGUIDE.markdown) examples for details

## 0.12.0
Allow post-aggregation filters (`HAVING` clauses).

- Added `having` method on `AggregationFeature`

## 0.11.0
Allow arbitrary feature value serialisation.

- Changed `EavtSink` to `EavtTextSink`.
- Created a `TextSink` that takes a `Thrift` struct (describing the sink format),
 and an implicit `FeatureValueEnc[T]` (that defines how to transform a
 `FeatureValue` into the `Thrift` struct via `encode((FeatureValue, Time) => T)`).

 ### Upgrading
 References to `EavtSink` should be changed to `EavtTextSink`.

## 0.10.0
Better support for running jobs with multiple feature sets. Also
checks for absense of `_SUCCESS` file prior to writing features to sink,
writing the file at the end of the job once all data is written.

- Changed return type of `FeatureSink.write`. Implementations must now
 return the set `Path`s written to so they can be committed (`_SUCCESS`
 file written) at the end of the job. While not enforced, implementations
 should also check that paths are not already committed when writing new
 data.
- Moved `Partitions` instance from `ScaldingDataSource` into top-level
 `commbank.coppersmith.scalding` package and made directly available
 via api import.

 ### Upgrading
 Custom sink implementations need to return the set of `Path`s written
 to from the `write` method. If writing to a single, fixed partition, the
 new FixedPartition case class could be used as sink's partition, making
 it straight forward to retrieve the path written to.

 References to `commbank.coppersmith.scalding.ScaldingDataSource.Partitions`
 should be replaced with `commbank.coppersmith.scalding.Partitions`.

## 0.9.0
- Move implicits into a separate object (`Coppersmith`) within the API
  package object.

## 0.8.0
- Require at least one value when creating a `ScaldingDataSource.Partitions`
 instance.

 ### Upgrading
 There shouldn't be any changes required to working code, as creating a
 `Partitions` instance without any values would have resulted in a job
 that does not read any data from the source instance. The only case
 where this might not be true is for an unpartitioned data source. In
 that case, the `ScaldingDataSource.Partitions.unpartitioned` instance
 should be used. Any other build failure should just require at least
 one partition value to be supplied.

## 0.7.0
- Renamed `HydroSink` to `EavtSink`.

## 0.6.0
- Remove `sourceTag` and `valueTag` from `Metadata` case class due to
 serialisation problems.

 ### Upgrading
 Source and value type information can now be retrieved from `sourceType`
 and `valueType` respectively.


## 0.5.0
- Remove `HydroSink.configure(MaestroConfig, String, DelimiterConflictStrategy)`

 ### Upgrading

 If you are using a `MaestroConfig` object to configure HydroSink, then replace:

    HydroSink.configure(maestro, dbPrefix, dcs)

 with:

    HydroSink.configure(dbPrefix, new Path(maestro.hdfsRoot), maestro.tablename, maestro.source.some, dcs)


## 0.4.0
- No API changes
- Bumped compile-time hadoop dependencies from cdh5.2.4 to cdh5.3.8


## 0.3.0
- Added feature context to the `FeatureSet` time functions. Allows
 feature sets to have feature times derived from either data or
 the feature generation time (coming from the context). The default
 implementation comes from the context.

 ### Upgrading

 To upgrade, ensure that the `time` method on feature sets overrides the
 parent and takes two parameters: `S`, and `FeatureSet`. Usually the default
 implementation is fine so  upgrading is simply a case of deleting the `time`
 method from your `FeatureSet`.
