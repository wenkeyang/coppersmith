package commbank.coppersmith.spark


// We won't be importing the maestro API for the spark implementation so collect
//imports here
object commonImports {
  type ThriftStruct    = com.twitter.scrooge.ThriftStruct
  type Hdfs[A] = au.com.cba.omnia.permafrost.hdfs.Hdfs[A]
  val Hdfs = au.com.cba.omnia.permafrost.hdfs.Hdfs

  type Partition[A,B] = au.com.cba.omnia.maestro.core.partition.Partition[A,B]
  val Partition = au.com.cba.omnia.maestro.core.partition.Partition

  type Hive[A] = au.com.cba.omnia.beeswax.Hive[A]
  val Hive = au.com.cba.omnia.beeswax.Hive

  type Decode[A]       = au.com.cba.omnia.maestro.core.codec.Decode[A]

  type Splitter        = au.com.cba.omnia.maestro.core.split.Splitter
  val  Splitter        = au.com.cba.omnia.maestro.core.split.Splitter

  type Path = org.apache.hadoop.fs.Path
}
