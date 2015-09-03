package commbank.coppersmith

import scalaz.syntax.std.option.ToOptionIdOps

import Feature._
import Join._

abstract class FeatureSource[S, FS <: FeatureSource[S, FS]](filter: Option[S => Boolean] = None) {
  self: FS =>

  def featureSetBuilder(namespace: Namespace, entity: S => EntityId, time: S => Time) =
    FeatureSetBuilder(namespace, entity, time)

  def filter(p: S => Boolean): FS =
    copyWithFilter(filter.map(f => (s: S) => f(s) && p(s)).orElse(p.some))

  def copyWithFilter(filter: Option[S => Boolean]): FS

  def configure[B <: SourceBinder[S, FS, P], P[_] : Lift](binder: B): ConfiguredFeatureSource[S, FS, P] = {
    implicitly[Lift[P]].liftBinder(this, binder, filter)
  }
}

trait ConfiguredFeatureSource[S, U, P[_]] {
  def load: P[S]
}

trait SourceBinder[S, U, P[_]] {
  def bind(underlying: U): P[S]
}

object SourceBinder extends SourceBinderInstances

trait SourceBinderInstances {
  def from[S, P[_]](dataSource: DataSource[S, P]) = FromBinder(dataSource)

  def join[L, R, J : Ordering, P[_] : Lift](leftSource: DataSource[L, P], rightSource: DataSource[R, P]) =
    JoinedBinder(leftSource, rightSource)

  def leftJoin[L, R, J : Ordering, P[_] : Lift](leftSource: DataSource[L, P], rightSource: DataSource[R, P]) =
    LeftJoinedBinder(leftSource, rightSource)
}

case class FromBinder[S, P[_]](src: DataSource[S, P]) extends SourceBinder[S, From[S], P]{
  def bind(from: From[S]): P[S] = {
    src.load
  }
}

case class JoinedBinder[L, R, J : Ordering, P[_] : Lift](
  leftSrc:  DataSource[L, P],
  rightSrc: DataSource[R, P]
) extends SourceBinder[(L, R), Joined[L, R, J, (L, R), Inner[L, R]], P] {
  def bind(j: Joined[L, R, J, (L, R), Inner[L, R]]): P[(L, R)] = {
    implicitly[Lift[P]].liftJoin(j)(leftSrc.load, rightSrc.load)
  }
}

case class LeftJoinedBinder[L, R, J : Ordering, P[_] : Lift](
  leftSrc:  DataSource[L, P],
  rightSrc: DataSource[R, P]
) extends SourceBinder[(L, Option[R]), Joined[L, R, J, (L, Option[R]), LeftOuter[L, R]], P] {
  def bind(j: Joined[L, R, J, (L, Option[R]), LeftOuter[L, R]]): P[(L, Option[R])] = {
    implicitly[Lift[P]].liftLeftJoin(j)(leftSrc.load, rightSrc.load)
  }
}
