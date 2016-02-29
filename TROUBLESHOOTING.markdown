Troubleshooting Coppersmith
===========================

### Serialisation issues

The functions that make up feature definitions must only refer to fully
serialisable types. Unfortunately it can be easy to accidentally close
over some non-serialisable instance indirectly, especially if using
classes that are nested within another class. Doing so will usually
manifest itself at runtime in the form of an exception with a stacktrace
along the lines of:

```
...
Caused by: cascading.flow.planner.PlannerException: could not build flow from assembly: [Neither Java nor Kyro works for class: class com.twitter.scalding.typed.MapFn instance: <function1>
export CHILL_EXTERNALIZER_DEBUG=true to see both stack traces]
...
Caused by: java.lang.RuntimeException: Neither Java nor Kyro works for class: class com.twitter.scalding.typed.MapFn instance: <function1>
export CHILL_EXTERNALIZER_DEBUG=true to see both stack traces
...
```

As indicated by the message, if you set the `CHILL_EXTERNALIZER_DEBUG` environment variable to `true` and run the job again, you will receive more information about the unserialisable instance, eg:

```
com.esotericsoftware.kryo.KryoException: java.util.ConcurrentModificationException
Serialization trace:
classes (sun.misc.Launcher$AppClassLoader)
classloader (java.security.ProtectionDomain)
context (java.security.AccessControlContext)
acc (java.net.URLClassLoader)
classloader (java.security.ProtectionDomain)
context (java.security.AccessControlContext)
acc (java.net.URLClassLoader)
classLoader (scala.reflect.runtime.JavaMirrors$JavaMirror)
$outer (scala.reflect.runtime.JavaMirrors$JavaMirror$$anon$1)
currentOwner (scala.reflect.internal.Trees$TreeTypeSubstituter)
EmptyTreeTypeSubstituter (scala.reflect.runtime.JavaUniverse)
$outer (scala.reflect.api.TypeTags$TypeTagImpl)
commbank$coppersmith$AggregationFeature$$evidence$8 (commbank.coppersmith.AggregationFeature)
collectF (commbank.coppersmith.scalding.ScaldingJobSpec$ScaldingJobSpec$AggregationFeatures$)
features$2 (commbank.coppersmith.scalding.SimpleFeatureJobOps$$anonfun$2)
g$1 (com.twitter.scalding.typed.TypedPipe$$anonfun$groupBy$1)
fn (com.twitter.scalding.typed.MapFn)
fmap (com.twitter.scalding.typed.MapFn)
```

Working from the bottom of the serialisation trace up should reveal the
type at the edge of the feature definition code that is causing the
problem (`ScaldingJobSpec.AggregationFeatures` in the above example).

In the case of a nested class that is implicitly carrying a reference to
its outer instance, it is usually sufficient to make the nested class a
top level class instead. In the case of classes that have
non-serialisable fields, making them transient if possible or marking
them as transient can also fix the problem. More information can be
found in this [scalding FAQ](https://github.com/twitter/scalding/wiki/Frequently-asked-questions#q-im-getting-a-notserializableexception-on-hadoop-job-submission).
