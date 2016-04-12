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
import com.twitter.scalding.{Execution, TupleConverter, TupleSetter}

import org.apache.hadoop.fs.Path

import scalaz.std.list.listInstance
import scalaz.syntax.bind.ToBindOps
import scalaz.syntax.functor.ToFunctorOps
import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.traverse.ToTraverseOps

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.scalding.ConfHelper.createUniqueFilenames

import Partitions.PathComponents
import FeatureSink.{AttemptedWriteToCommitted, WriteResult}

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
  ): WriteResult = {

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
          oPValues    <- partitioned.aggregate(Aggregator.toSet.composePrepare(_._1)).toOptionExecution
          oPartitions  = oPValues.map(_.toSet.toList).toList.flatten.toNel.map(pValues =>
                           Partitions(conf.partition, pValues.head, pValues.tail: _*)
                         )
          result      <- moveToTarget(tempDir, conf.path, oPartitions)
          _           <- Execution.fromHive(ensureTextTableExists(conf))
        } yield result
      }.ensure(Execution.fromHdfs(Hdfs.delete(tempDir, recDelete = true)))
    })
  }

  private def moveToTarget(
    sourceDir:  Path,
    targetDir:  Path,
    partitions: Option[Partitions[_]]
  ): WriteResult = {
    val partitionRelPaths = partitions.map(_.relativePaths).getOrElse(List())

    partitionRelPaths.foldLeft[WriteResult](Execution.from(Right(Set())))((acc, partitionRelPath) =>
      acc.flatMap {
        case l@Left(_) => Execution.from(l)
        case Right(pathsSoFar) => {
          val sourcePartition = new Path(sourceDir, partitionRelPath)
          val targetPartition = new Path(targetDir, partitionRelPath)
          FeatureSink.isCommitted(targetPartition).flatMap {
            case true => Execution.from(Left(AttemptedWriteToCommitted(targetPartition)))
            case false =>
              Execution.fromHdfs(
                for {
                  parts <- Hdfs.glob(sourcePartition, "*")
                  _     <- Hdfs.mkdirs(targetPartition)
                  _     <- parts.map(part =>
                             Hdfs.move(part, new Path(targetPartition, part.getName))
                           ).sequence
                } yield Right(pathsSoFar + targetPartition)
              )
          }
        }
      }
    )
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
