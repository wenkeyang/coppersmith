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

package commbank.coppersmith.api

import com.twitter.scalding._
import commbank.coppersmith.Lift

package object scalding {

  type FeatureJobConfig[S] = commbank.coppersmith.scalding.FeatureJobConfig[S]
  type FeatureSetExecution = commbank.coppersmith.scalding.FeatureSetExecution
  type FeatureSetExecutions = commbank.coppersmith.scalding.FeatureSetExecutions
  type FeatureSink = commbank.coppersmith.scalding.FeatureSink
  type FixedSinkPartition[T, P] = commbank.coppersmith.scalding.FixedSinkPartition[T, P]
  type Partitions[P] = commbank.coppersmith.scalding.Partitions[P]
  type PathComponents[P] = commbank.coppersmith.scalding.Partitions.PathComponents[P]
  type SimpleFeatureJob = commbank.coppersmith.scalding.SimpleFeatureJob
  type SimpleFeatureJobOps = commbank.coppersmith.scalding.SimpleFeatureJobOps
  type FeatureValueEnc[T] = commbank.coppersmith.scalding.FeatureValueEnc[T]

  val DataSourceView = commbank.coppersmith.scalding.DataSourceView
  val FeatureSetExecutions = commbank.coppersmith.scalding.FeatureSetExecutions
  val FeatureSetExecution = commbank.coppersmith.scalding.FeatureSetExecution
  val Partitions = commbank.coppersmith.scalding.Partitions
  val HiveTextSource = commbank.coppersmith.scalding.HiveTextSource
  val HiveParquetSource = commbank.coppersmith.scalding.HiveParquetSource
  val TypedPipeSource = commbank.coppersmith.scalding.TypedPipeSource
  val FixedSinkPartition = commbank.coppersmith.scalding.FixedSinkPartition
  val DerivedSinkPartition = commbank.coppersmith.scalding.DerivedSinkPartition
  val HiveTextSink = commbank.coppersmith.scalding.HiveTextSink
  val HiveParquetSink = commbank.coppersmith.scalding.HiveParquetSink
  val EavtText = commbank.coppersmith.scalding.EavtText

  implicit val framework: Lift[TypedPipe] = commbank.coppersmith.scalding.lift.scalding
}
