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
import com.twitter.scalding.{Execution, TupleSetter}

import org.apache.hadoop.fs.Path
import org.joda.time.DateTime

import scalaz.NonEmptyList
import scalaz.std.list.listInstance
import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.traverse.ToTraverseOps

import au.com.cba.omnia.maestro.api._, Maestro._

import commbank.coppersmith.Feature._, Value._
import commbank.coppersmith.FeatureValue

import commbank.coppersmith.thrift.Eavt

import Partitions.PathComponents
import HiveSupport.{DelimiterConflictStrategy, FailJob}

import FeatureSink.WriteResult

trait FeatureSink {
  /**
    * Persist feature values, returning the list of paths written (for committing at the
    * end of the job) or an error if trying to write to a path that is already committed
    */
  def write(features: TypedPipe[(FeatureValue[_], Time)]): WriteResult
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

sealed trait SinkPartition[T] {
  type P
  def pathComponents: PathComponents[P]
  def tupleSetter: TupleSetter[P]
  def underlying: Partition[T, P]
}

final case class FixedSinkPartition[T, PP : PathComponents : TupleSetter](
  fieldNames: List[String],
  pathPattern: String,
  partitionValue: PP
) extends SinkPartition[T] {
  type P = PP
  def pathComponents = implicitly
  def tupleSetter = implicitly
  def underlying = Partition(fieldNames, _ => partitionValue, pathPattern)
}

final case class DerivedSinkPartition[T, PP : PathComponents : TupleSetter](
  underlying: Partition[T, PP]
) extends SinkPartition[T] {
  type P = PP
  def pathComponents = implicitly
  def tupleSetter = implicitly
}

object EavtSink {
  type DatabaseName = String
  type TableName    = String

  val NullValue = "\\N"
  val Delimiter = "|"

  import HiveSupport.HiveConfig

  val defaultPartition = DerivedSinkPartition[Eavt, (String, String, String)](
    HivePartition.byDay(Fields[Eavt].Time, "yyyy-MM-dd")
  )

  def configure(dbPrefix:  String,
                dbRoot:    Path,
                tableName: TableName,
                partition: SinkPartition[Eavt] = defaultPartition,
                group:     Option[String] = None,
                dcs:       DelimiterConflictStrategy[Eavt] = FailJob[Eavt]()): EavtSink =
    EavtSink(
      Config(
        s"${dbPrefix}_features",
        new Path(dbRoot, s"view/warehouse/features/${group.map(_ + "/").getOrElse("")}$tableName"),
        tableName,
        partition,
        dcs
      )
    )

  case class Config(
    dbName:    DatabaseName,
    tablePath: Path,
    tableName: TableName,
    partition: SinkPartition[Eavt],
    dcs:       DelimiterConflictStrategy[Eavt] = FailJob[Eavt]()
  ) {
    def hiveConfig =
      HiveSupport.HiveConfig[Eavt, partition.P](
        partition.underlying,
        dbName,
        tablePath,
        tableName,
        EavtSink.Delimiter,
        dcs
      )
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
    val eavtPipe = features.map { case (fv, t) => EavtSink.toEavt(fv, t) }

    implicit val pathComponents: PathComponents[conf.partition.P] = conf.partition.pathComponents
    // Note: This needs to be explicitly specified so that the TupleSetter.singleSetter
    // instance isn't used (causing a failure at runtime).
    implicit val tupleSetter: TupleSetter[conf.partition.P] = conf.partition.tupleSetter
    HiveSupport.writeTextTable(conf.hiveConfig, eavtPipe)
  }
}
