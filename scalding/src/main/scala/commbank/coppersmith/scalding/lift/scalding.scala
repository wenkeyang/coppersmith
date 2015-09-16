package commbank.coppersmith.scalding.lift

import com.twitter.scalding._
import commbank.coppersmith.Join.JoinedHl

import commbank.coppersmith._, Feature.Value
import commbank.coppersmith.scalding.ScaldingBoundFeatureSource

import shapeless._
import shapeless.ops.hlist.Prepend


trait ScaldingLift extends Lift[TypedPipe] {

  def lift[S, V <: Value](f:Feature[S,V])(s: TypedPipe[S]): TypedPipe[FeatureValue[V]] = {
    s.flatMap(s => f.generate(s))
  }

  def lift[S](fs: FeatureSet[S])(s: TypedPipe[S]): TypedPipe[FeatureValue[_]] = {
    s.flatMap(s => fs.generate(s))
  }

  def liftJoinHl[LeftSides <: HList, RightSide, J : Ordering, Out <: HList, PrevJoins <: HList]
    (joined: JoinedHl[LeftSides, RightSide, RightSide, PrevJoins, J, Out ])
    (a:TypedPipe[LeftSides], b: TypedPipe[RightSide])
    (implicit pp: Prepend.Aux[LeftSides, RightSide :: HNil, Out])
    : TypedPipe[Out] = {
    val result = a.groupBy(joined.l).join(b.groupBy(joined.r)).values.map {case (hl, r) => hl :+ r }
    result
  }
  def liftLeftJoinHl[LeftSides <: HList, RightSide, J : Ordering, O <: HList, PrevJoins <: HList]
    (joined: JoinedHl[LeftSides, Option[RightSide], RightSide, PrevJoins, J, O])
    (a:TypedPipe[LeftSides], b: TypedPipe[RightSide])
    (implicit pp: Prepend.Aux[LeftSides, Option[RightSide] :: HNil, O])
      : TypedPipe[O] = {
    a.groupBy(joined.l).leftJoin(b.groupBy(joined.r)).values.map {case (hl, r) => hl :+ r}
  }

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, TypedPipe]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ) = ScaldingBoundFeatureSource(underlying, binder, filter)
}

object scalding extends ScaldingLift
