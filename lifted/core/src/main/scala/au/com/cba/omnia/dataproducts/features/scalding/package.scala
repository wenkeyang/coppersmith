package au.com.cba.omnia.dataproducts.features

import au.com.cba.omnia.dataproducts.features.Feature.Value
import com.twitter.scalding._

package object scalding {
  def liftToTypedPipe[S,V <: Value](f:Feature[S,V])(s: TypedPipe[S]): TypedPipe[FeatureValue[S, V]] = {
    s.flatMap(s => f.generate(s))
  }
  def materialise[S,V <: Value](f:Feature[S,V])(src:TypedPipe[S], sink: TypedSink[FeatureValue[S, V]]): Execution[Unit] = {
    val pipe = liftToTypedPipe(f)(src)
    pipe.writeExecution(sink)
  }
}
