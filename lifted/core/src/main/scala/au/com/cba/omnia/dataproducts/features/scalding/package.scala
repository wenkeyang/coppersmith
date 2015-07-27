package au.com.cba.omnia.dataproducts.features

import au.com.cba.omnia.dataproducts.features.Feature.Value

import com.twitter.scalding._

package object scalding {
  def liftToTypedPipe[S,V <: Value](f:Feature[S,V])(s: TypedPipe[S]): TypedPipe[FeatureValue[S, V]] = {
    s.flatMap(s => f.generate(s))
  }

  def liftToTypedPipe[S](fs: FeatureSet[S])(s: TypedPipe[S]): TypedPipe[FeatureValue[S, _]] = {
    s.flatMap(s => fs.generate(s))
  }

  def materialise[S,V <: Value](f:Feature[S,V])(src:TypedPipe[S], sink: TypedSink[FeatureValue[S, V]]): Execution[Unit] = {
    val pipe = liftToTypedPipe(f)(src)
    pipe.writeExecution(sink)
  }

  def materialise[S](featureSet: FeatureSet[S])(src:TypedPipe[S], sink: TypedSink[FeatureValue[S, _]]): Execution[Unit] = {
    val pipe = liftToTypedPipe(featureSet)(src)
    pipe.writeExecution(sink)
  }


}
