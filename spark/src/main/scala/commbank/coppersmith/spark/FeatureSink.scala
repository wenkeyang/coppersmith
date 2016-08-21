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

package commbank.coppersmith
package spark

import org.apache.hadoop.fs.Path

import scalaz.NonEmptyList
import scalaz.std.list.listInstance
import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.traverse.ToTraverseOps

import com.twitter.util.Encoder

import org.joda.time.DateTime

import Feature._

import Partitions.PathComponents

import FeatureSink.{MetadataWriter, WriteResult}

import org.apache.spark.rdd.RDD

import commonImports._

import Action.actionInstance.monadSyntax._

trait FeatureSink {
  /**
    * Persist feature values, returning the list of paths written (for committing at the
    * end of the job) or an error if trying to write to a path that is already committed
    */
  def write(features: RDD[(FeatureValue[Value], FeatureTime)],
            metadataSet: MetadataSet[Any]): WriteResult

  def metadataWriter: MetadataWriter

  def writeMetadata(metadataSet: MetadataSet[Any], paths: Set[Path]) =
    metadataWriter(metadataSet, paths)
}

object FeatureSink {
  sealed trait WriteError
  case class AlreadyCommitted(paths: NonEmptyList[Path]) extends WriteError
  case class AttemptedWriteToCommitted(path: Path) extends WriteError

  type WriteResult = Action[Either[WriteError, Set[Path]]]
  type MetadataWriter = (MetadataSet[Any], Set[Path]) => WriteResult

  val defaultMetadataWriter: MetadataWriter = (metadataSet, paths) => {
    val metadataOut = MetadataOutput.Json1
    val metadata = metadataOut.stringify(metadataOut.doOutput(List(metadataSet), Conforms.allConforms))
    val metadataFileName = s"_feature_metadata/_${metadataSet.name}_METADATA.V${metadataOut.version}.json"

    val writes: Action[List[Unit]] = paths.map { p =>
      val f = new Path(p, metadataFileName)
      Action.fromHdfs(Hdfs.write(f, metadata))
    }.toList.sequence
    writes.map(_ => Right(paths))
  }

  def commitFlag(path: Path) = new Path(path, "_SUCCESS")
  def isCommitted(path: Path): Action[Boolean] = Action.fromHdfs(Hdfs.exists(commitFlag(path)))

  type CommitResult = Action[Either[WriteError, Unit]]
  // Note: Check for committed flags and subsequent writing thereof is not atomic
  def commit(paths: Set[Path]): CommitResult = {

    // Check all paths for committed state first. Avoids committing earlier paths
    // if a latter path is already committed and would fail the job overall.
    val pathCommits: Action[List[(Path, Boolean)]] =
      paths.toList.map(p => isCommitted(p).map((p, _))).sequence

    pathCommits.flatMap(pathCommitStates => {
      val committedPaths = pathCommitStates.collect { case (path, true) => path }
      committedPaths.toNel.map(committed =>
        Action.pure(Left(AlreadyCommitted(committed)))
      ).getOrElse(
        paths.toList.map(path =>
          Action.fromHdfs(Hdfs.create(commitFlag(path)))
        ).sequence.map(n => Right(()))
        /* used to be sequence.unit.map(Right(_)). ask dkristian what unit does */
      )
    })
  }
}

sealed trait SinkPartition[T] {
  type P
  def pathComponents: PathComponents[P]
  def underlying: Partition[T, P]
}

final case class FixedSinkPartition[T, PP : PathComponents](
  fieldNames: List[String],
  pathPattern: String,
  partitionValue: PP
) extends SinkPartition[T] {
  type P = PP
  def pathComponents = implicitly
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

final case class DerivedSinkPartition[T, PP : PathComponents](
  underlying: Partition[T, PP]
) extends SinkPartition[T] {
  type P = PP
  def pathComponents = implicitly
}

trait FeatureValueEnc[T] extends Encoder[(FeatureValue[Value], FeatureTime), T] {
  def encode(fvt: (FeatureValue[Value], FeatureTime)): T
}
