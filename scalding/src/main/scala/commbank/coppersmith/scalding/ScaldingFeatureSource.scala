package commbank.coppersmith.scalding

import com.twitter.scalding._

import commbank.coppersmith.{ConfiguredFeatureSource, SourceBinder}

case class ScaldingConfiguredFeatureSource[S, U](
  underlying: U,
  binder: SourceBinder[S, U, TypedPipe],
  filter: Option[S => Boolean]
) extends ConfiguredFeatureSource[S, U, TypedPipe] {
  def load: TypedPipe[S] = {
    val pipe = binder.bind(underlying)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}
