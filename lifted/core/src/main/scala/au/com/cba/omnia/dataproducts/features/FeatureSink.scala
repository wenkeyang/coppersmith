package au.com.cba.omnia.dataproducts.features

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, Execution, MultipleTextLineFiles, TupleSetter, TupleConverter}
import com.twitter.scalding.typed._

import au.com.cba.omnia.maestro.api._, Maestro._

import au.com.cba.omnia.etl.util.{ParseUtils, SimpleMaestroJob}

trait FeatureSink {
  def write(features: TypedPipe[FeatureValue[_, _]]): Execution[Unit]
}


case class HydroSink(conf: FeatureJobConfig) extends FeatureSink {
  def write(features: TypedPipe[FeatureValue[_, _]]) =
???
    // for {
    //   _ <- HiveSupport.writeTextTable(conf.hiveEavtTextConfig, eavtThriftPipe)
    //   _ <- Execution.fromHdfs {
    //        for {
    //          files <- Hdfs.glob(new Path(conf.hiveEavtTextConfig.path, "*/*"))
    //          _     <- files.traverse_[Hdfs](eachPath => for {
    //                     r1  <- Hdfs.create(s"${eachPath.toString}/_SUCCESS".toPath)
    //                   } yield ())
    //          } yield()
    //   }
    // } yield()
}

// Maestro's HiveTable currently assumes the underlying format to be Parquet. This code generalises
// code from different feature gen projects, which supports storing the final EAVT records as text.
object HiveSupport {
  case class HiveConfig[T <: ThriftStruct with Product : Manifest, P](
    partition: Partition[T, P],
    database:  String,
    path:      Path,
    tablename: String,
    delimiter: String = "|"
  )

  def writeTextTable[T <: ThriftStruct with Product : Manifest, P : TupleSetter : TupleConverter](
    conf: HiveConfig[T, P],
    eavtPipe: TypedPipe[T]
  ): Execution[Unit] = {

    import conf.partition
    val partitioned = eavtPipe.map(v => partition.extract(v) -> ParseUtils.mkStringThrift[T](v, conf.delimiter))
    val partitionPath = partition.fieldNames.map(_ + "=%s").mkString("/")
    for {
      _ <- partitioned.writeExecution(PartitionedTextLine[P](conf.path.toString, partitionPath))
      _ <- Execution.fromHive(createTextTable(conf))
    } yield ()
  }

  def createTextTable[T <: ThriftStruct with Product : Manifest](conf: HiveConfig[T, _]): Hive[Unit] =
???
  //   for {
  //   _ <- Hive.createTextTable[T](
  //         database         = conf.database,
  //         table            = conf.tablename,
  //         partitionColumns = conf.partition.fieldNames.map(_ -> "string"),
  //         location         = Option(conf.path),
  //         delimiter        = conf.delimiter
  //       )
  //   _ <- Hive.queries(List(s"use ${conf.database}", s"msck repair table ${conf.tablename}"))
  // } yield ()
}
