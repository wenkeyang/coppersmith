package commbank.coppersmith

import scalaz.syntax.std.option.ToOptionIdOps

import Join._

abstract class FeatureSource[S, FS <: FeatureSource[S, FS]](filter: Option[S => Boolean] = None) {
  self: FS =>

  def filter(p: S => Boolean): FS = copyWithFilter(filter.map(f => (s: S) => f(s) && p(s)).orElse(p.some))

  def copyWithFilter(filter: Option[S => Boolean]): FS

  def bind[P[_] : Lift](binder: SourceBinder[S, FS, P]): BoundFeatureSource[S, P] = {
    implicitly[Lift[P]].liftBinder(self, binder, filter)
  }
}

trait BoundFeatureSource[S, P[_]] {
  def load: P[S]
}

trait SourceBinder[S, U, P[_]] {
  def bind(underlying: U): P[S]
}

object SourceBinder extends SourceBinderInstances

trait SourceBinderInstances {
  def from[S, P[_] : Lift](dataSource: DataSource[S, P]) = FromBinder(dataSource)

  def join[L, R, J : Ordering, P[_] : Lift](leftSrc: DataSource[L, P], rightSrc: DataSource[R, P]) =
    JoinedBinder(leftSrc, rightSrc)

  def leftJoin[L, R, J : Ordering, P[_] : Lift](leftSrc: DataSource[L, P], rightSrc: DataSource[R, P]) =
    LeftJoinedBinder(leftSrc, rightSrc)
}

case class FromBinder[S, P[_]](src: DataSource[S, P]) extends SourceBinder[S, From[S], P] {
  def bind(from: From[S]): P[S] = src.load
}

case class JoinedBinder[L, R, J : Ordering, P[_] : Lift](
  leftSrc:  DataSource[L, P],
  rightSrc: DataSource[R, P]
) extends SourceBinder[(L, R), Joined[L, R, J, (L, R)], P] {
  def bind(j: Joined[L, R, J, (L, R)]): P[(L, R)] = {
    implicitly[Lift[P]].liftJoin(j)(leftSrc.load, rightSrc.load)
  }
}

case class LeftJoinedBinder[L, R, J : Ordering, P[_] : Lift](
  leftSrc:  DataSource[L, P],
  rightSrc: DataSource[R, P]
) extends SourceBinder[(L, Option[R]), Joined[L, R, J, (L, Option[R])], P] {
  def bind(j: Joined[L, R, J, (L, Option[R])]): P[(L, Option[R])] = {
    implicitly[Lift[P]].liftLeftJoin(j)(leftSrc.load, rightSrc.load)
  }
}
