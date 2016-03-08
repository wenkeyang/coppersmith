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

import commbank.coppersmith.Feature.Value
import scala.reflect.runtime.universe.TypeTag

package object api {

  implicit def fromFS[S](fs: FeatureSource[S, _]) =
    commbank.coppersmith.FeatureBuilderSource.fromFS(fs)
  implicit def fromCFS[S, C: TypeTag](fs: ContextFeatureSource[S, C, _]) =
    commbank.coppersmith.FeatureBuilderSource.fromCFS(fs)

  def from[S, P[_] : Lift](dataSource: DataSource[S, P]) =
    commbank.coppersmith.SourceBinder.from(dataSource)
  def join[L, R, J : Ordering, P[_] : Lift](leftSrc: DataSource[L, P], rightSrc: DataSource[R, P]) =
    commbank.coppersmith.SourceBinder.join(leftSrc, rightSrc)
  def leftJoin[L, R, J : Ordering, P[_] : Lift](leftSrc: DataSource[L, P], rightSrc: DataSource[R, P]) =
    commbank.coppersmith.SourceBinder.leftJoin(leftSrc, rightSrc)

  type Feature[S, +V <: Value] = commbank.coppersmith.Feature[S, V]
  type FeatureSet[S] = commbank.coppersmith.FeatureSet[S]
  type FeatureSetWithTime[S] = commbank.coppersmith.FeatureSetWithTime[S]
  type MetadataSet[S] = commbank.coppersmith.MetadataSet[S]
  type AggregationFeature[S, SV, U, +V <: Value] = commbank.coppersmith.AggregationFeature[S, SV, U, V]
  type AggregationFeatureSet[S] = commbank.coppersmith.AggregationFeatureSet[S]
  type Integral = commbank.coppersmith.Feature.Value.Integral
  type Str = commbank.coppersmith.Feature.Value.Str
  type Decimal = commbank.coppersmith.Feature.Value.Decimal
  type BasicFeatureSet[S] = commbank.coppersmith.BasicFeatureSet[S]
  type QueryFeatureSet[S, V <: Value] = commbank.coppersmith.QueryFeatureSet[S, V]
  type FeatureContext = commbank.coppersmith.FeatureContext
  type PivotFeatureSet[S] = commbank.coppersmith.PivotFeatureSet[S]
  type ContextFeatureSource[S, C, FS <: FeatureSource[S, FS]] = commbank.coppersmith.ContextFeatureSource[S, C, FS]

  val FeatureStub = commbank.coppersmith.FeatureStub
  val ExplicitGenerationTime = commbank.coppersmith.ExplicitGenerationTime
  val From = commbank.coppersmith.From
  val Join = commbank.coppersmith.Join
  val Continuous = commbank.coppersmith.Feature.Type.Continuous
  val Discrete = commbank.coppersmith.Feature.Type.Discrete
  val Nominal = commbank.coppersmith.Feature.Type.Nominal
  val Ordinal = commbank.coppersmith.Feature.Type.Ordinal
  val Metadata = commbank.coppersmith.Feature.Metadata
  val FeatureValue = commbank.coppersmith.FeatureValue
  val PivotMacro = commbank.coppersmith.PivotMacro
  val MinMaxRange = Value.MinMaxRange
  val SetRange = Value.SetRange
}
