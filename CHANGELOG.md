Change log
==========

## 0.3.0
- Added feature context to the `FeatureSet` time functions. Allows
 feature sets to have feature times derived from either data or
 the feature generation time (coming from the context). The default
 implementation comes from the context. To upgrade, ensure that the `time`
 method on feature sets overrides the parent and takes two parameters:
 `S`, and `FeatureSet`. Usually the default implementation is fine so 
 upgrading is simply a case of deleting the `time` method from your
 `FeatureSet`.
