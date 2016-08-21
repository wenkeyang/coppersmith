package commbank.coppersmith

import org.apache.spark.rdd.RDD

package object spark {
  implicit val framework: Lift[RDD] = spark.lift.spark
}
