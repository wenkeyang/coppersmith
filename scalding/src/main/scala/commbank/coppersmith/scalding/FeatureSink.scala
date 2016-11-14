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

import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{Execution, TupleSetter, TupleConverter}
import com.twitter.util.Encoder

import org.apache.hadoop.fs.Path

import scalaz.Scalaz._
import scalaz.NonEmptyList
import scalaz.std.list.listInstance
import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.traverse.ToTraverseOps

import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api._, Maestro._

import commbank.coppersmith._, Feature._

import Partitions.PathComponents
import HiveSupport.{DelimiterConflictStrategy, FailJob}

import FeatureSink.WriteResult
import MetadataSink.MetadataWriter

trait FeatureSink {
  /**
    * Persist feature values, returning the list of paths written (for committing at the
    * end of the job) or an error if trying to write to a path that is already committed
    */
  def write(features: TypedPipe[(FeatureValue[Value], FeatureTime)],
            metadataSet: MetadataSet[Any]): WriteResult
}

object FeatureSink {
  sealed trait WriteError
  case class AlreadyCommitted(paths: NonEmptyList[Path]) extends WriteError
  case class AttemptedWriteToCommitted(path: Path) extends WriteError

  type WriteResult = Execution[Either[WriteError, Set[Path]]]

  def commitFlag(path: Path) = new Path(path, "_SUCCESS")
  def isCommitted(path: Path): Execution[Boolean] = Execution.fromHdfs(Hdfs.exists(commitFlag(path)))

  type CommitResult = Execution[Either[WriteError, Unit]]
  // Note: Check for committed flags and subsequent writing thereof is not atomic
  def commit(paths: Set[Path]): CommitResult = {

    // Check all paths for committed state first. Avoids committing earlier paths
    // if a latter path is already committed and would fail the job overall.
    val pathCommits: Execution[List[(Path, Boolean)]] =
      paths.toList.map(p => isCommitted(p).map((p, _))).sequence

    pathCommits.flatMap(pathCommitStates => {
      val committedPaths = pathCommitStates.collect { case (path, true) => path }
      committedPaths.toNel.map(committed =>
        Execution.from(Left(AlreadyCommitted(committed)))
      ).getOrElse(
        paths.toList.map(path =>
          Execution.fromHdfs(Hdfs.create(commitFlag(path)))
        ).sequence.unit.map(Right(_))
      )
    })
  }
}

class MetadataSink(metadataWriter: MetadataWriter)(underlying: FeatureSink) extends FeatureSink {
  def write(features: TypedPipe[(FeatureValue[Value], FeatureTime)], metadataSet: MetadataSet[Any]) = {
    underlying.write(features, metadataSet).flatMap(_ match {
      case Left(x) => Execution.from(Left(x))
      case Right(paths) => {
        metadataWriter(metadataSet, paths)
      }
    })
  }
}

object MetadataSink {
  type MetadataWriter = (MetadataSet[Any], Set[Path]) => WriteResult

  val defaultMetadataWriter: MetadataWriter = (metadataSet, paths) => {
    val metadataOut = MetadataOutput.Json1
    val metadata = metadataOut.stringify(metadataOut.doOutput(List(metadataSet), Conforms.allConforms))
    val metadataFileName = s"_feature_metadata/_${metadataSet.name}_METADATA.V${metadataOut.version}.json"

    val writes: Execution[List[Unit]] = paths.map { p =>
      val f = new Path(p, metadataFileName)
      Execution.fromHdfs(Hdfs.write(f, metadata))
    }.toList.sequence
    writes.map(_ => Right(paths))
  }

  def apply(underlying: FeatureSink): MetadataSink = new MetadataSink(defaultMetadataWriter)(underlying)
}

sealed trait SinkPartition[T] {
  type P
  def pathComponents: PathComponents[P]
  def tupleSetter: TupleSetter[P]
  def tupleConverter: TupleConverter[P]
  def underlying: Partition[T, P]
}

final case class FixedSinkPartition[T, PP : PathComponents : TupleSetter : TupleConverter](
  fieldNames: List[String],
  pathPattern: String,
  partitionValue: PP
) extends SinkPartition[T] {
  type P = PP
  def pathComponents = implicitly
  def tupleSetter = implicitly
  def tupleConverter = implicitly
  def underlying = Partition(fieldNames, _ => partitionValue, pathPattern)
}

object FixedSinkPartition {
  def byDay[T](dt: DateTime) =
    FixedSinkPartition[T, (String, String, String)](
      List("year", "month", "day"),
      "year=%s/month=%s/day=%s",
      (dt.getYear.toString, f"${dt.getMonthOfYear}%02d", f"${dt.getDayOfMonth}%02d")
    )
}

final case class DerivedSinkPartition[T, PP : PathComponents : TupleSetter : TupleConverter](
  underlying: Partition[T, PP]
) extends SinkPartition[T] {
  type P = PP
  def pathComponents = implicitly
  def tupleSetter = implicitly
  def tupleConverter = implicitly
}

trait FeatureValueEnc[T] extends Encoder[(FeatureValue[Value], FeatureTime), T] {
  def encode(fvt: (FeatureValue[Value], FeatureTime)): T
}

object HiveTextSink {
  type DatabaseName = String
  type TableName    = String

  val NullValue = "\\N"
  val Delimiter = "|"
}

case class HiveTextSink[
  T <: ThriftStruct with Product : FeatureValueEnc : Manifest
](
   dbName:        HiveTextSink.DatabaseName,
   tablePath:     Path,
   tableName:     HiveTextSink.TableName,
   partition:     SinkPartition[T],
   delimiter:     String = HiveTextSink.Delimiter,
   dcs:           DelimiterConflictStrategy[T] = FailJob[T]()
) extends FeatureSink {
  def write(features: TypedPipe[(FeatureValue[Value], FeatureTime)], metadataSet: MetadataSet[Any]) = {
    val textPipe = features.map(implicitly[FeatureValueEnc[T]].encode)

    val hiveConfig =
      HiveSupport.HiveConfig[T, partition.P](
        partition.underlying,
        dbName,
        tablePath,
        tableName,
        delimiter,
        dcs
      )

    implicit val pathComponents: PathComponents[partition.P] = partition.pathComponents
    // Note: These needs to be explicitly specified so that the TupleSetter.singleSetter and
    // TupleConverter.singleConverter instances aren't used (causing a failure at runtime).
    implicit val tupleSetter: TupleSetter[partition.P] = partition.tupleSetter
    implicit val tupleConverter: TupleConverter[partition.P] = partition.tupleConverter

    HiveSupport.writeTextTable(hiveConfig, textPipe)
  }
}
