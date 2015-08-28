package commbank.coppersmith

import org.joda.time.DateTime

import scalaz.std.list.listInstance
import scalaz.syntax.bind.ToBindOps
import scalaz.syntax.functor.ToFunctorOps
import scalaz.syntax.foldable.ToFoldableOps

import org.apache.hadoop.fs.Path

import com.twitter.algebird.Aggregator

import com.twitter.scalding.{Execution, TupleSetter, TupleConverter}
import com.twitter.scalding.typed.{PartitionedTextLine, TypedPipe}

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.api._, Maestro._

import Feature.Value.{Integral, Decimal, Str}

import thrift.Eavt

trait FeatureSink {
  def write(features: TypedPipe[FeatureValue[_]]): Execution[Unit]
}

object HydroSink {
  type DatabaseName = String
  type TableName    = String

  val NullValue = "\\N"

  import HiveSupport.HiveConfig

  val partition = Partition.byDate(Fields[Eavt].Time, "yyyy-MM-dd")
  def config(maestro: MaestroConfig, dbRawPrefix: String) = Config(
      HiveConfig(
        partition,
        s"${dbRawPrefix}_features",
        new Path(s"${maestro.hdfsRoot}/view/warehouse/features/${maestro.source}/${maestro.tablename}"),
        maestro.tablename
      )
  )

  def config(databaseName: DatabaseName, databasePath: Path, tableName: TableName) =
    Config(HiveConfig(partition, databaseName, databasePath, tableName))

  case class Config(hiveConfig: HiveConfig[Eavt, (String, String, String)])

  def toEavt(fv: FeatureValue[_]) = {
    val featureValue = (fv.value match {
      case Integral(v) => v.map(_.toString)
      case Decimal(v)  => v.map(_.toString)
      case Str(v)      => v
    }).getOrElse(NullValue)

    // TODO: Does time format need to be configurable?
    val featureTime = new DateTime(fv.time).toString("yyyy-MM-dd")
    Eavt(fv.entity, fv.name, featureValue, featureTime)
  }
}

case class HydroSink(conf: HydroSink.Config) extends FeatureSink {

  def write(features: TypedPipe[FeatureValue[_]]) = {
    val hiveConfig = conf.hiveConfig
    val eavtPipe = features.map(HydroSink.toEavt)
    for {
      partitions <- HiveSupport.writeTextTable(conf.hiveConfig, eavtPipe)
      _          <- Execution.fromHdfs {
                      for {
                        // FIXME: Derive path(s) from job
                        files <- Hdfs.glob(new Path(hiveConfig.path, "*/*/*"))
                        _     <- files.traverse_[Hdfs](eachPath => for {
                                   r1  <- Hdfs.create(s"${eachPath.toString}/_SUCCESS".toPath)
                                 } yield ())
                      } yield()
      }
    } yield()
  }
}

// Maestro's HiveTable currently assumes the underlying format to be Parquet. This code generalises
// code from different feature gen projects, which supports storing the final EAVT records as text.
// Won't be required once Hydro moves to Parquet format
object HiveSupport {
  case class HiveConfig[T <: ThriftStruct with Product : Manifest, P](
    partition: Partition[T, P],
    database:  String,
    path:      Path,
    tablename: String,
    delimiter: String = "\u0001"
  )

  def writeTextTable[T <: ThriftStruct with Product : Manifest, P : TupleSetter : TupleConverter](
    conf: HiveConfig[T, P],
    pipe: TypedPipe[T]
  ): Execution[Set[P]] = {

    import conf.partition
    val partitioned: TypedPipe[(P, String)] = pipe.map(v =>
      partition.extract(v) -> serialise[T](v, conf.delimiter, "\\N")
    )
    val partitionPath = partition.fieldNames.map(_ + "=%s").mkString("/")
    for {
      _          <- partitioned.writeExecution(PartitionedTextLine[P](conf.path.toString, partitionPath))
      _          <- Execution.fromHive(createTextTable(conf))
      partitions <- partitioned.aggregate(Aggregator.toSet.composePrepare(_._1)).toOptionExecution
    } yield partitions.toSet.flatten
  }

  def createTextTable[T <: ThriftStruct with Product : Manifest](conf: HiveConfig[T, _]): Hive[Unit] =
    for {
      _ <- Hive.createTextTable[T](
             database         = conf.database,
             table            = conf.tablename,
             partitionColumns = conf.partition.fieldNames.map(_ -> "string"),
             location         = Option(conf.path),
             delimiter        = conf.delimiter
           )
      _ <- Hive.queries(List(s"use ${conf.database}", s"msck repair table ${conf.tablename}"))
    } yield ()

  // Adapted from ParseUtils in util.etl project
  private def serialise[T <: Product](row: T, sep: String, none: String): String =
    row.productIterator.map(value => {
      val result = value match {
        case Some(x) => x
        case None    => none
        case any     => any
      }
      if (result.toString.contains(sep)) {
        sys.error((s"field $result in $row contains the specified delimiter $sep"))
      } else {
        result
      }
    }).mkString(sep)
}
