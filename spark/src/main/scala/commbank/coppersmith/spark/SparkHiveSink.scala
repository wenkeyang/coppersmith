package commbank.coppersmith
package spark

import org.apache.spark.rdd.RDD

import FeatureSink._
import Feature._

import commonImports._

class SparkHiveSink[T <: ThriftStruct] (namespace: String, path: Path, table: String) extends FeatureSink {
  def write(
    features: RDD[(FeatureValue[Value], FeatureTime)],
    metadataSet: MetadataSet[Any]):WriteResult = {
      Action.pure(Right({
        features.foreach(println)
        //features.saveAsTextFile(s"${path.toString}/${table}")
        Set(new Path(path, table))
      }))
    }

    def metadataWriter = ???
}


object SparkHiveSink {
  def apply[T <: ThriftStruct](
    namespace: String, path: Path, table:String
  ): SparkHiveSink[T] = new SparkHiveSink(namespace, path, table)
}
