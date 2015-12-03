Change log
==========

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
