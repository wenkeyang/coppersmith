//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package commbank.coppersmith.scalding

import com.twitter.scalding.{Execution, TupleSetter, TypedPipe}

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api._

import commbank.coppersmith._, Feature._
import Partitions.PathComponents
import FeatureSink.{AttemptedWriteToCommitted, MetadataWriter}

/**
  * Parquet FeatureSink implementation - create using HiveParquetSink.apply in companion object.
  */
case class HiveParquetSink[T <: ThriftStruct : Manifest : FeatureValueEnc, P : TupleSetter] private(
  table:         HiveTable[T, (P, T)],
  partitionPath: Path,
  metadataWriter: MetadataWriter
) extends FeatureSink {
  def write(features: TypedPipe[(FeatureValue[Value], FeatureTime)],
            metadataSet: MetadataSet[Any]): FeatureSink.WriteResult = {
    FeatureSink.isCommitted(partitionPath).flatMap(committed =>
      if (committed) {
        Execution.from(Left(AttemptedWriteToCommitted(partitionPath)))
      } else {
        val eavts = features.map(implicitly[FeatureValueEnc[T]].encode)

        table.writeExecution(eavts).flatMap { _ =>
          writeMetadata(metadataSet, Set(partitionPath))
        }
      }
    )
  }
}

object HiveParquetSink {
  type DatabaseName = String
  type TableName    = String

  def apply[
    T <: ThriftStruct : Manifest : FeatureValueEnc,
    P : Manifest : PathComponents
  ](
    dbName:    DatabaseName,
    tableName: TableName,
    tablePath: Path,
    partition: FixedSinkPartition[T, P],
    writeMetadata: MetadataWriter = FeatureSink.defaultMetadataWriter
  ): HiveParquetSink[T, P] = {
    // Note: This needs to be explicitly specified so that the TupleSetter.singleSetter
    // instance isn't used (causing a failure at runtime).
    implicit val partitionSetter = partition.tupleSetter
    val hiveTable = HiveTable[T, P](
      dbName,
      tableName,
      partition.underlying,
      tablePath.toString
    )

    val pathComponents = implicitly[PathComponents[P]].toComponents(partition.partitionValue)
    val partitionRelPath = new Path(partition.underlying.pattern.format(pathComponents: _*))

    HiveParquetSink[T, P](hiveTable, new Path(tablePath, partitionRelPath), writeMetadata)
  }
}
