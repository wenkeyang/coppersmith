package commbank.coppersmith
package spark

import org.apache.spark.rdd.RDD

import FeatureSink._
import Feature._

import commonImports._

import scala.reflect.ClassTag

class SparkHiveSink[T <: ThriftStruct : ClassTag] (
  namespace: String, path: Path, table: String) extends FeatureSink {
  def write(
    features: RDD[(FeatureValue[Value], FeatureTime)],
    metadataSet: MetadataSet[Any]):WriteResult = {
      val encoded = features.map(EavtText.EavtEnc.encode).map {eavt =>
        s"${eavt.entity}|${eavt.attribute}|${eavt.value}|${eavt.time}" // this needs real doing
      }
      Action.pure(Right({
        encoded.saveAsTextFile(s"${path.toString}/${table}")
        Set(new Path(path, table))
      }))
    }

    def metadataWriter = ???
}


object SparkHiveSink {
  val NullValue = "\\N"

  def apply[T <: ThriftStruct : FeatureValueEnc : ClassTag](
    namespace: String, path: Path, table:String
  ): SparkHiveSink[T] = new SparkHiveSink(namespace, path, table)
}
