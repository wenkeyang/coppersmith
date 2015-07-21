package au.com.cba.omnia.dataproducts.features

import au.com.cba.omnia.dataproducts.features.Feature.Value

package object memory {
  def liftToMemory[S,V <: Value](f:Feature[S,V])(s: S): Option[FeatureValue[S, V]] = f.generate(s)
}
