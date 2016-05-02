Coppersmith Plugin
==================

The coppersmith plugin automatically adds coppersmith to a project's library dependencies
and generates metadata as a published artifact.

To install the plugin, add the following to `project/plugins.sbt`:

```scala
addSbtPlugin("au.com.cba.omnia" %% "coppersmith-plugin" % "<coppersmith-version>")
```

where `coppersmith-version` is the version of coppersmith you want to use.

From there, ```metadata:export``` will generate a metadata JSON file, and the
artifacts published by `publishLocal` and `publish` will include the metadata
as an artifact.
