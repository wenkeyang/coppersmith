package commbank.coppersmith.scalding

import com.twitter.scalding._

import commbank.coppersmith.{BoundFeatureSource, FeatureSource, SourceBinder}

case class ScaldingBoundFeatureSource[S, U <: FeatureSource[S, U]](
  underlying: U,
  binder: SourceBinder[S, U, TypedPipe],
  filter: Option[S => Boolean]
) extends BoundFeatureSource[S, TypedPipe] {
  def load: TypedPipe[S] = {
    val pipe = binder.bind(underlying)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}
