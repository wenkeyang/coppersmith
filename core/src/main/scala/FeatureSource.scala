package commbank.coppersmith

import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.scalding.typed.TypedPipe

import lift.scalding._

import Join._

case class FeatureSource[S, U, B <: SourceBinder[S, U]](underlying: U, filter: Option[S => Boolean] = None) {
  import Feature._
  def featureSetBuilder(namespace: Namespace, entity: S => EntityId, time: S => Time) =
    FeatureSetBuilder(namespace, entity, time)

  def filter(p: S => Boolean): FeatureSource[S, U, B] =
    copy(filter = filter.map(f => (s: S) => f(s) && p(s)).orElse(p.some))

  def configure(binder: B): ConfiguredFeatureSource[S, U] = {
    ConfiguredFeatureSource(underlying, binder, filter)
  }
}

object FeatureSource extends FeatureSourceInstances

trait FeatureSourceInstances {
  implicit def fromFS[S](s: From[S]) =
    FeatureSource[S, From[S], FromBinder[S]](s)

  implicit def joinFS[L, R, J : Ordering](s: Joined[L, R, J, Inner]) =
    FeatureSource[(L, R), Joined[L, R, J, Inner], JoinedBinder[L, R, J]](s)

  implicit def leftFS[L, R, J : Ordering](s: Joined[L, R, J, LeftOuter]) =
    FeatureSource[(L, Option[R]), Joined[L, R, J, LeftOuter], LeftJoinedBinder[L, R, J]](s)
}

case class ConfiguredFeatureSource[S, U](
  underlying: U,
  binder: SourceBinder[S, U],
  filter: Option[S => Boolean]
) {
  def load(conf: FeatureJobConfig[S]): TypedPipe[S] = {
    val pipe = binder.bind(underlying, conf)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}

trait SourceBinder[S, U] {
  def bind(underlying: U, cfg: FeatureJobConfig[S]): TypedPipe[S]
}

object SourceBinder extends SourceBinderInstances

trait SourceBinderInstances {
  def from[S](dataSource: SourceConfiguration[S]) = FromBinder(dataSource)

  def join[L, R, J : Ordering](
    leftSource: SourceConfiguration[L],
    rightSource: SourceConfiguration[R]
  ) = JoinedBinder(leftSource, rightSource)

  def leftJoin[L, R, J : Ordering](
    leftSource: SourceConfiguration[L],
    rightSource: SourceConfiguration[R]
  ) =
    LeftJoinedBinder(leftSource, rightSource)
}

case class FromBinder[S](src: SourceConfiguration[S]) extends SourceBinder[S, From[S]] {
  def bind(from: From[S], conf: FeatureJobConfig[S]): TypedPipe[S] = {
    src.load(conf)
  }
}

case class JoinedBinder[L, R, J : Ordering](
  leftSrc:  SourceConfiguration[L],
  rightSrc: SourceConfiguration[R]
) extends SourceBinder[(L, R), Joined[L, R, J, Inner]] {
  def bind(j: Joined[L, R, J, Inner], conf: FeatureJobConfig[(L, R)]): TypedPipe[(L, R)] = {
    liftJoin(j)(leftSrc.load(conf), rightSrc.load(conf))
  }
}

case class LeftJoinedBinder[L, R, J : Ordering](
  leftSrc:  SourceConfiguration[L],
  rightSrc: SourceConfiguration[R]
) extends SourceBinder[(L, Option[R]), Joined[L, R, J, LeftOuter]] {
  def bind(j: Joined[L, R, J, LeftOuter],
           conf: FeatureJobConfig[(L, Option[R])]): TypedPipe[(L, Option[R])] = {
    liftLeftJoin(j)(leftSrc.load(conf), rightSrc.load(conf))
  }
}
