//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith.scalding

import com.twitter.algebird.Aggregator
import com.twitter.scalding.typed.{PartitionedTextLine, TypedPipe}
import com.twitter.scalding.{Config, Execution, TupleConverter, TupleSetter}

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.fs.Path
import org.joda.time.DateTime

import scalaz.std.list.listInstance
import scalaz.syntax.bind.ToBindOps
import scalaz.syntax.foldable.ToFoldableOps
import scalaz.syntax.functor.ToFunctorOps
import scalaz.syntax.std.option.ToOptionIdOps
import scalaz.syntax.traverse.ToTraverseOps

import au.com.cba.omnia.maestro.api.Maestro._
import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.scalding.ConfHelper.createUniqueFilenames

import commbank.coppersmith.Feature.Value._
import commbank.coppersmith.Feature._

import commbank.coppersmith.FeatureValue
import commbank.coppersmith.scalding.ScaldingDataSource.{Partitions, PathComponents}
import commbank.coppersmith.thrift.Eavt

import HiveSupport.{DelimiterConflictStrategy, FailJob}

trait FeatureSink {
  def write(features: TypedPipe[(FeatureValue[_], Time)]): Execution[Unit]
}

object EavtSink {
  type DatabaseName = String
  type TableName    = String

  val NullValue = "\\N"
  val Delimiter = "|"

  import HiveSupport.HiveConfig

  val partition = HivePartition.byDay(Fields[Eavt].Time, "yyyy-MM-dd")

  def configure(dbPrefix:  String,
                dbRoot:    Path,
                tableName: TableName,
                group:     Option[String] = None,
                dcs:       DelimiterConflictStrategy[Eavt] = FailJob[Eavt]()): EavtSink =
    EavtSink(
      Config(
        s"${dbPrefix}_features",
        new Path(dbRoot, s"view/warehouse/features/${group.map(_ + "/").getOrElse("")}$tableName"),
        tableName,
        dcs
      )
    )

  case class Config(
    dbName:    DatabaseName,
    tablePath: Path,
    tableName: TableName,
    dcs:       DelimiterConflictStrategy[Eavt] = FailJob[Eavt]()
  ) {
    def hiveConfig =
      HiveConfig[Eavt, (String, String, String)](partition, dbName, tablePath, tableName, Delimiter, dcs)
  }

  def toEavt(fv: FeatureValue[_], time: Time) = {
    val featureValue = (fv.value match {
      case Integral(v) => v.map(_.toString)
      case Decimal(v)  => v.map(_.toString)
      case Str(v)      => v
    }).getOrElse(NullValue)

    // TODO: Does time format need to be configurable?
    val featureTime = new DateTime(time).toString("yyyy-MM-dd")
    Eavt(fv.entity, fv.name, featureValue, featureTime)
  }
}

case class EavtSink(conf: EavtSink.Config) extends FeatureSink {
  def write(features: TypedPipe[(FeatureValue[_], Time)]) = {
    val hiveConfig = conf.hiveConfig
    val eavtPipe = features.map { case (fv, t) => EavtSink.toEavt(fv, t) }

    for {
      partitions <- HiveSupport.writeTextTable(hiveConfig, eavtPipe)
      _          <- Execution.fromHdfs {
                      partitions.toPaths(hiveConfig.path).toList.traverse_[Hdfs](eachPath =>
                        Hdfs.create(s"${eachPath.toString}/_SUCCESS".toPath).map(_ => ())
                      )
                    }
    } yield ()
  }
}

// Maestro's HiveTable currently assumes the underlying format to be Parquet. This code generalises
// code from different feature gen projects, which supports storing the final EAVT records as text.
object HiveSupport {
  trait DelimiterConflictStrategy[T] {
    def handle(row: T, result: String, sep: String): Option[String]
  }
  // Use if field values are assumed to never contain the separator character
  case class FailJob[T]() extends DelimiterConflictStrategy[T] {
    def handle(row: T, result: String, sep: String) =
      sys.error(s"field '$result' in '$row' contains the specified delimiter '$sep'")
  }

  case class HiveConfig[T <: ThriftStruct : Manifest, P](
    partition: Partition[T, P],
    database:  String,
    path:      Path,
    tablename: String,
    delimiter: String,
    dcs:       DelimiterConflictStrategy[T]
  )

  def writeTextTable[
    T <: ThriftStruct with Product : Manifest,
    P : TupleSetter : TupleConverter : PathComponents
  ](
    conf: HiveConfig[T, P],
    pipe: TypedPipe[T]
  ): Execution[Partitions[P]] = {

    import conf.partition
    val partitioned: TypedPipe[(P, String)] = pipe.map(v =>
      partition.extract(v) -> serialise[T](v, conf.delimiter, "\\N")(conf.dcs)
    )

    // Use an intermediate temporary directory to write pipe in order to avoid race condition that
    // occurs when two parallel executions are writing to the same sink in two different Cascading
    // Flow instances. Race condition occurs when shared _temporary directory is deleted via
    // cascading.tap.hadoop.util.Hadoop18TapUtil.cleanupJob when Hadoop18TapUtil.isInflow returns
    // false and before the other execution has copied its results.
    Execution.fromHdfs(Hdfs.createTempDir()).flatMap(tempDir => {
      val sink = PartitionedTextLine(tempDir.toString, partition.pattern);
      {
        for {
                         // Use append semantics for now as an interim fix to address #97
                         // Check if this is still relevant once #137 is addressed
          _           <- partitioned.writeExecution(sink).withSubConfig(createUniqueFilenames(_))
          oPartitions <- partitioned.aggregate(Aggregator.toSet.composePrepare(_._1)).toOptionExecution
          partitions   = Partitions(conf.partition, oPartitions.toSet.flatten.toList: _*)
          _           <- moveToTarget(tempDir, conf.path, partitions)
          _           <- Execution.fromHive(ensureTextTableExists(conf))
        } yield partitions
      }.ensure(Execution.fromHdfs(Hdfs.delete(tempDir, recDelete = true)))
    })
  }

  private def moveToTarget(
    sourceDir:  Path,
    targetDir:  Path,
    partitions: Partitions[_]
  ): Execution[Unit] = {
    val partitionRelPaths = partitions.relativePaths
    partitionRelPaths.map(partitionRelPath => {
      val sourcePartition = new Path(sourceDir, partitionRelPath)
      val targetPartition = new Path(targetDir, partitionRelPath)
      Execution.fromHdfs(
        for {
          parts <- Hdfs.glob(sourcePartition, "*")
          _     <- Hdfs.mkdirs(targetPartition)
          _     <- parts.map(part => Hdfs.move(part, new Path(targetPartition, part.getName))).sequence
        } yield ()
      )
    }).sequence.unit
  }

  def ensureTextTableExists[T <: ThriftStruct : Manifest](conf: HiveConfig[T, _]): Hive[Unit] =
    for {
           // Hive.createTextTable is idempotent - it will no-op if the table already exists,
           // assuming the schema matches exactly
      _ <- Hive.createTextTable[T](
             database         = conf.database,
             table            = conf.tablename,
             partitionColumns = conf.partition.fieldNames.map(_ -> "string"),
             location         = Option(conf.path),
             delimiter        = conf.delimiter
           )
      _ <- Hive.queries(List(s"use `${conf.database}`", s"msck repair table `${conf.tablename}`"))
    } yield ()

  // Adapted from ParseUtils in util.etl project
  private def serialise[T <: Product](row: T, sep: String, none: String)
                                     (implicit dcs: DelimiterConflictStrategy[T]): String = {

    def delimiterConflict(s: String) = s.contains(sep)

    row.productIterator.flatMap(value => {
      val result = value match {
        case Some(x) => x
        case None    => none
        case any     => any
      }
      if (delimiterConflict(result.toString)) {
        dcs.handle(row, value.toString, sep).map(r =>
          if (delimiterConflict(r)) {
            sys.error(("Delimiter conflict not handled adequately: " +
                       s"result '$r' from '$row' still contains the specified delimiter '$sep'"))
          } else {
            r
          }
        )
      } else {
        Option(result)
      }
    }).mkString(sep)
  }
}
