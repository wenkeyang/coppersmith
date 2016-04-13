Change log
==========

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
