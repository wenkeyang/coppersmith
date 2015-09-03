package commbank.coppersmith

import commbank.coppersmith.Feature.Value
import commbank.coppersmith.Join._
import commbank.coppersmith.Feature.Value
import commbank.coppersmith.Join.{LeftOuter, Inner, Joined}

trait Lift[P[_]] {
  def lift[S, V <: Value](f:Feature[S,V])(s: P[S]): P[FeatureValue[V]]

  def lift[S](fs: FeatureSet[S])(s: P[S]): P[FeatureValue[_]]


  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, B), Inner[A, B]])(a: P[A], b: P[B]): P[(A, B)]

  def liftLeftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, Option[B]), LeftOuter[A, B]])(a: P[A], b: P[B]): P[(A, Option[B])]

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, P]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ): ConfiguredFeatureSource[S, U, P]
}
