package commbank.coppersmith

import Join._
import Feature._
import com.twitter.scalding._

import lift.scalding._

import scalaz.syntax.std.option.ToOptionIdOps

case class FeatureSource[S, U, B <: SourceBinder[S, U]](underlying: U, filter: Option[S => Boolean] = None) {
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
  def load: TypedPipe[S] = {
    val pipe = binder.bind(underlying)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}

trait SourceBinder[S, U] {
  def bind(underlying: U): TypedPipe[S]
}

object SourceBinder extends SourceBinderInstances

trait SourceBinderInstances {
  def from[S](dataSource: DataSource[S]) = FromBinder(dataSource)

  def join[L, R, J : Ordering](leftSource: DataSource[L], rightSource: DataSource[R]) =
    JoinedBinder(leftSource, rightSource)

  def leftJoin[L, R, J : Ordering](leftSource: DataSource[L], rightSource: DataSource[R]) =
    LeftJoinedBinder(leftSource, rightSource)
}

case class FromBinder[S](src: DataSource[S]) extends SourceBinder[S, From[S]]{
  def bind(from: From[S]): TypedPipe[S] = {
    src.load
  }
}

case class JoinedBinder[L, R, J : Ordering](
  leftSrc:  DataSource[L],
  rightSrc: DataSource[R]
) extends SourceBinder[(L, R), Joined[L, R, J, Inner]] {
  def bind(j: Joined[L, R, J, Inner]): TypedPipe[(L, R)] = {
    liftJoin(j)(leftSrc.load, rightSrc.load)
  }
}

case class LeftJoinedBinder[L, R, J : Ordering](
  leftSrc:  DataSource[L],
  rightSrc: DataSource[R]
) extends SourceBinder[(L, Option[R]), Joined[L, R, J, LeftOuter]] {
  def bind(j: Joined[L, R, J, LeftOuter]): TypedPipe[(L, Option[R])] = {
    liftLeftJoin(j)(leftSrc.load, rightSrc.load)
  }
}
