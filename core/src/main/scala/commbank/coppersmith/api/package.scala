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

import scala.reflect.runtime.universe.TypeTag

import commbank.coppersmith.Feature.Value
import commbank.coppersmith.generated.GeneratedJoinTypeInstances

package object api extends GeneratedJoinTypeInstances with SourceBinderInstances {

  object Coppersmith extends au.com.cba.omnia.maestro.macros.MacroSupport {
    implicit def fromFS[S](fs: FeatureSource[S, _]): FeatureBuilderSource[S] =
      commbank.coppersmith.FeatureBuilderSource.fromFS(fs)

    implicit def fromCFS[S, C: TypeTag](fs: ContextFeatureSource[S, C, _]): FeatureBuilderSource[(S, C)] =
      commbank.coppersmith.FeatureBuilderSource.fromCFS(fs)
  }

  type AbstractFeatureSet[S] = commbank.coppersmith.AbstractFeatureSet[S]
  type Feature[S, +V <: Value] = commbank.coppersmith.Feature[S, V]
  type FeatureSet[S] = commbank.coppersmith.FeatureSet[S]
  @deprecated("Use FeatureSet", "0.22.0")
  type FeatureSetWithTime[S] = commbank.coppersmith.FeatureSet[S]
  type MetadataSet[S] = commbank.coppersmith.MetadataSet[S]
  type AggregationFeature[S, SV, U, +V <: Value] = commbank.coppersmith.AggregationFeature[S, SV, U, V]
  type AggregationFeatureSet[S] = commbank.coppersmith.AggregationFeatureSet[S]
  type Integral = commbank.coppersmith.Feature.Value.Integral
  type Str = commbank.coppersmith.Feature.Value.Str
  type Bool = commbank.coppersmith.Feature.Value.Bool
  type Decimal = commbank.coppersmith.Feature.Value.Decimal
  type FloatingPoint = commbank.coppersmith.Feature.Value.FloatingPoint
  type Date = commbank.coppersmith.Feature.Value.Date
  type Time = commbank.coppersmith.Feature.Value.Time
  type BasicFeatureSet[S] = commbank.coppersmith.BasicFeatureSet[S]
  type QueryFeatureSet[S, V <: Value] = commbank.coppersmith.QueryFeatureSet[S, V]
  type FeatureContext = commbank.coppersmith.FeatureContext
  type PivotFeatureSet[S] = commbank.coppersmith.PivotFeatureSet[S]
  type ContextFeatureSource[S, C, FS <: FeatureSource[S, FS]] = commbank.coppersmith.ContextFeatureSource[S, C, FS]
  type DataSource[S, P[_]] = commbank.coppersmith.DataSource[S, P]
  type FeatureValue[+V <: Value] = commbank.coppersmith.FeatureValue[V]
  type FeatureTime = commbank.coppersmith.Feature.FeatureTime
  type Value = commbank.coppersmith.Feature.Value

  // Maestro dependencies below
  type JobStatus = au.com.cba.omnia.maestro.api.JobStatus
  type Fields[A] = au.com.cba.omnia.maestro.macros.FieldsMacro.Fields[A]
  type Encode[A] = au.com.cba.omnia.maestro.core.codec.Encode[A]

  val Integral = commbank.coppersmith.Feature.Value.Integral
  val Str = commbank.coppersmith.Feature.Value.Str
  val Bool = commbank.coppersmith.Feature.Value.Bool
  val Decimal = commbank.coppersmith.Feature.Value.Decimal
  val FloatingPoint = commbank.coppersmith.Feature.Value.FloatingPoint
  val Date = commbank.coppersmith.Feature.Value.Date
  val Time = commbank.coppersmith.Feature.Value.Time
  val FeatureStub = commbank.coppersmith.FeatureStub
  val ExplicitGenerationTime = commbank.coppersmith.ExplicitGenerationTime
  val From = commbank.coppersmith.From
  val Join = commbank.coppersmith.Join
  val Continuous = commbank.coppersmith.Feature.Type.Continuous
  val Discrete = commbank.coppersmith.Feature.Type.Discrete
  val Nominal = commbank.coppersmith.Feature.Type.Nominal
  val Ordinal = commbank.coppersmith.Feature.Type.Ordinal
  val Instant = commbank.coppersmith.Feature.Type.Instant
  val Metadata = commbank.coppersmith.Feature.Metadata
  val FeatureValue = commbank.coppersmith.FeatureValue
  val MinMaxRange = Value.MinMaxRange
  val SetRange = Value.SetRange
  val MapRange = Value.MapRange
  val Patterns = commbank.coppersmith.Patterns

  //Maestro dependencies below
  lazy val JobFinished =  au.com.cba.omnia.maestro.scalding.JobFinished
  lazy val HivePartition = au.com.cba.omnia.maestro.api.HivePartition
  lazy val Encode = au.com.cba.omnia.maestro.core.codec.Encode
}
