package commbank.coppersmith.api

import commbank.coppersmith.Lift

import org.apache.spark.rdd.RDD
import commbank.coppersmith.spark.commonImports._

package object spark {
  type FeatureJobConfig[S] = commbank.coppersmith.spark.FeatureJobConfig[S]

  val HiveTextSource = commbank.coppersmith.spark.HiveTextSource
  val HiveParquetSource = commbank.coppersmith.spark.HiveParquetSource

  implicit val framework: Lift[RDD] = commbank.coppersmith.spark.lift.spark

  type SparkHiveSink[T <: ThriftStruct] = commbank.coppersmith.spark.SparkHiveSink[T]
  val SparkHiveSink = commbank.coppersmith.spark.SparkHiveSink

  type SimpleFeatureJob = commbank.coppersmith.spark.SimpleFeatureJob
}
