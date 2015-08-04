package au.com.cba.omnia.dataproducts.features

import au.com.cba.omnia.dataproducts.features.Feature.Value
import au.com.cba.omnia.dataproducts.features.Join.Joined

import com.twitter.scalding._

package object scalding {
  def liftToTypedPipe[S,V <: Value](f:Feature[S,V])(s: TypedPipe[S]): TypedPipe[FeatureValue[S, V]] = {
    s.flatMap(s => f.generate(s))
  }

  def liftToTypedPipe[S](fs: FeatureSet[S])(s: TypedPipe[S]): TypedPipe[FeatureValue[S, _]] = {
    s.flatMap(s => fs.generate(s))
  }


  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J])(a:TypedPipe[A], b: TypedPipe[B]): TypedPipe[(A, B)] = {
    a.groupBy(joined.left).join(b.groupBy(joined.right)).values
  }

  def materialiseJoinFeature[A, B, J : Ordering, V <: Value]
    (joined: Joined[A, B, J], feature: Feature[(A,B),V])
    (leftSrc:TypedPipe[A], rightSrc:TypedPipe[B], sink: TypedSink[FeatureValue[(A,B),V]]) =
    materialise[(A,B), V](feature)(liftJoin(joined)(leftSrc, rightSrc), sink)

  def materialise[S,V <: Value](f:Feature[S,V])(src:TypedPipe[S], sink: TypedSink[FeatureValue[S, V]]): Execution[Unit] = {
    val pipe = liftToTypedPipe(f)(src)
    pipe.writeExecution(sink)
  }

  def materialise[S](featureSet: FeatureSet[S])(src:TypedPipe[S], sink: TypedSink[FeatureValue[S, _]]): Execution[Unit] = {
    val pipe = liftToTypedPipe(featureSet)(src)
    pipe.writeExecution(sink)
  }

}
