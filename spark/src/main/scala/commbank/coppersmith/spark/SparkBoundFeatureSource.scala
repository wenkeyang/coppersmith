package commbank.coppersmith
package spark

import org.apache.spark.rdd.RDD

case class SparkBoundFeatureSource[S, U <: FeatureSource[S, U]](
  underlying: U,
  binder: SourceBinder[S, U, RDD],
  filter: Option[S => Boolean]
) extends BoundFeatureSource[S, RDD] {
  def load: RDD[S] = {
    val pipe = binder.bind(underlying)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}
