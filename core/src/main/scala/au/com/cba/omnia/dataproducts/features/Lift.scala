package au.com.cba.omnia.dataproducts.features

import au.com.cba.omnia.dataproducts.features.Feature.Value
import au.com.cba.omnia.dataproducts.features.Join._

trait Lift[P[_]] {
  def lift[S, V <: Value](f:Feature[S,V])(s: P[S]): P[FeatureValue[V]]

  def lift[S](fs: FeatureSet[S])(s: P[S]): P[FeatureValue[_]]


  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J, Inner])(a: P[A], b: P[B]): P[(A, B)]

  def liftLeftJoin[A, B, J : Ordering](joined: Joined[A, B, J, LeftOuter])(a: P[A], b: P[B]): P[(A, Option[B])]

}

