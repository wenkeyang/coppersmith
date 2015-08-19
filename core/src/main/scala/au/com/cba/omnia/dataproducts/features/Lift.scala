package au.com.cba.omnia.dataproducts.features

import au.com.cba.omnia.dataproducts.features.Feature.Value
import au.com.cba.omnia.dataproducts.features.Join.Joined

trait Lift[P[_]] {
  def lift[S, V <: Value](f:Feature[S,V])(s: P[S]): P[FeatureValue[V]]

  def lift[S](fs: FeatureSet[S])(s: P[S]): P[FeatureValue[_]]


  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J])(a:P[A], b: P[B]): P[(A, B)]

}

trait Materialise[P[_], O[_], IO[_]] { this: Lift[P] =>
    def materialiseJoinFeature[A, B, J : Ordering, V <: Value]
        (joined: Joined[A, B, J], feature: Feature[(A,B),V])
        (leftSrc:P[A], rightSrc:P[B], sink: O[FeatureValue[V]]): IO[Unit]

    def materialise[S, V <: Value](f:Feature[S,V])(src:P[S], sink: O[FeatureValue[V]]): IO[Unit]

    def materialise[S](featureSet: FeatureSet[S])(src:P[S], sink: O[FeatureValue[_]]): IO[Unit]
}
