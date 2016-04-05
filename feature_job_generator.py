import os
import re
from collections import namedtuple

def remove_option(x):
  return x.replace("Option[", "").replace("]", "")

def lower_plural(t):
  return t.lower() + "s"

class FeatureSet:
  def __init__(self, name, set_type, type_params):
    self.name = name
    self.set_type = set_type
    self.type_params = type_params
    self.data_source_template = "  val %(val)-14s = HiveTextSource[%(type)s, Nothing](new Path(\"data/%(dir)s\"), partitions%(delim)s)"
    self.delims = { "Movie": "", "User": "", "Rating": ", \"\\t\"" }
  def split_types(self):
    return self.type_params.translate(None, "()").split(", ")
  def thrift_imports(self):
    types = map(remove_option, self.split_types())
    if len(types) == 1:
      return types[0]
    else:
      return "{{{}}}".format(", ".join(types))
  def data_sources(self):
    types = self.split_types()
    def map_data_sources(t):
      tx = remove_option(t)
      return self.data_source_template % { "val": lower_plural(tx), "type": tx, "dir": lower_plural(tx), "delim": self.delims[tx] }
    return "\n".join(map(map_data_sources, types))
  def binding(self):
    def val_names(types):
      return map(lower_plural, map(remove_option, types))
    types = self.split_types()
    type_str = ", ".join(val_names(types))
    if len(types) == 1:
      return "from(" + type_str + ")"
    elif "Option[" in types[1]:
      return "leftJoin(" + type_str + ")"
    else:
      return "join(" + type_str + ")"
  def table_name(self):
    return lower_plural(remove_option(self.split_types()[0]))


feature_set_pattern = "(\w+) extends (\w*FeatureSet\w*)\[((?:\([\w\,\s]+\))|(?:\w+))"
feature_job_pattern = "(\w+)Job extends \w+Job"

jobs = []

source_dir = './examples/target/scala-2.11/src_managed/main'
dest_dir = './examples/src/main/scala/commbank/coppersmith/examples'

files = [os.path.join(source_dir,f) for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir,f))]

for f in files:
  for line in open(f):
    feature_job = re.search(feature_job_pattern, line)
    if (feature_job is not None):
      jobs.append(feature_job.group(1))

sets = []
for f in files:
  for line in open(f):
    feature_set = re.search(feature_set_pattern, line)
    if (feature_set is not None and feature_set.group(1) not in jobs):
      sets.append(FeatureSet(feature_set.group(1), feature_set.group(2), feature_set.group(3)))


template = """package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Config

import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api.Maestro.DerivedDecode

import commbank.coppersmith.api._, scalding._
import commbank.coppersmith.examples.thrift.%(thrift_imports)s

case class %(name)sConfig(conf: Config) extends FeatureJobConfig[%(type_params)s] {
  val partitions     = ScaldingDataSource.Partitions.unpartitioned
%(data_sources)s

  val featureSource  = %(name)s.source.bind(%(binding)s)

  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))

  val featureSink    = EavtSink.configure("userguide", new Path("dev"), "%(table_name)s")
}

object %(name)sJob extends SimpleFeatureJob {
  def job = generate(%(name)sConfig(_), %(name)s)
}
"""

for s in sets:
  out = template % { 'thrift_imports': s.thrift_imports(),
               'name': s.name,
               'type_params': s.type_params,
               'data_sources': s.data_sources(),
               'binding': s.binding(),
               'table_name': s.table_name()
             }
  with open(os.path.join(dest_dir, s.name + "Job.scala"), "w+") as f:
    f.write(out)
