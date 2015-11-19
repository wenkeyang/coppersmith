package commbank.coppersmith

import commbank.coppersmith.Feature.Value
import scala.reflect.runtime.universe.TypeTag

package object api {

  implicit def fromFS[S](fs: FeatureSource[S, _]) = commbank.coppersmith.FeatureBuilderSource.fromFS[S](fs: FeatureSource[S, _])
  implicit def fromCFS[S, C: TypeTag](fs: ContextFeatureSource[S, C, _]) = commbank.coppersmith.FeatureBuilderSource.fromCFS[S, C](fs: ContextFeatureSource[S, C, _])

  def from[S, P[_] : Lift](dataSource: DataSource[S, P]) = commbank.coppersmith.SourceBinder.from[S, P](dataSource: DataSource[S, P])

  type Feature[S, +V <: Value] = commbank.coppersmith.Feature[S, V]
  type FeatureSet[S] = commbank.coppersmith.FeatureSet[S]
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
}