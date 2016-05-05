Change log
==========

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
