package commbank.coppersmith

import scalaz.syntax.std.option.ToOptionIdOps

import Feature._
import Join._

case class FeatureSource[S, U, B <: SourceBinder[S, U, P], P[_]](underlying: U,
                                                                 filter: Option[S => Boolean] = None) {
  def featureSetBuilder(namespace: Namespace, entity: S => EntityId, time: S => Time) =
    FeatureSetBuilder(namespace, entity, time)

  def filter(p: S => Boolean): FeatureSource[S, U, B, P] =
    copy(filter = filter.map(f => (s: S) => f(s) && p(s)).orElse(p.some))

  def configure(binder: B)(implicit lift: Lift[P]): ConfiguredFeatureSource[S, U, P] = {
    lift.liftBinder(underlying, binder, filter)
  }
}

object FeatureSource extends FeatureSourceInstances

trait FeatureSourceInstances {
  implicit def fromFS[S, P[_]](s: From[S]) =
    FeatureSource[S, From[S], FromBinder[S, P], P](s)

  implicit def joinFS[L, R, J : Ordering, P[_]](s: Joined[L, R, J, Inner]) =
    FeatureSource[(L, R), Joined[L, R, J, Inner], JoinedBinder[L, R, J, P], P](s)

  implicit def leftFS[L, R, J : Ordering, P[_]](s: Joined[L, R, J, LeftOuter]) =
    FeatureSource[(L, Option[R]), Joined[L, R, J, LeftOuter], LeftJoinedBinder[L, R, J, P], P](s)
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
) extends SourceBinder[(L, R), Joined[L, R, J, Inner], P] {
  def bind(j: Joined[L, R, J, Inner]): P[(L, R)] = {
    implicitly[Lift[P]].liftJoin(j)(leftSrc.load, rightSrc.load)
  }
}

case class LeftJoinedBinder[L, R, J : Ordering, P[_] : Lift](
  leftSrc:  DataSource[L, P],
  rightSrc: DataSource[R, P]
) extends SourceBinder[(L, Option[R]), Joined[L, R, J, LeftOuter], P] {
  def bind(j: Joined[L, R, J, LeftOuter]): P[(L, Option[R])] = {
    implicitly[Lift[P]].liftLeftJoin(j)(leftSrc.load, rightSrc.load)
  }
}
