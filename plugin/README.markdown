Coppersmith Plugin
==================

The coppersmith plugin generates metadata as a published artifact.

To install the plugin, add the following to `project/plugins.sbt`:

    addSbtPlugin("au.com.cba.omnia" %% "coppersmith-plugin" % "<coppersmith-version>")

and add the following to `build.sbt`:

    libraryDependencies ++= Seq("au.com.cba.omnia" %% "coppersmith-core"     % "<coppersmith-version>",
                                "au.com.cba.omnia" %% "coppersmith-scalding" % "<coppersmith-version>",
                                "au.com.cba.omnia" %% "coppersmith-tools"    % "<coppersmith-version>")

, where `<coppersmith-version>` is replaced with the version number of
coppersmith you want to use.

From there, ```metadata:export``` will generate a metadata JSON file, and the
artifacts published by `publishLocal` and `publish` will include the metadata
as an artifact.
