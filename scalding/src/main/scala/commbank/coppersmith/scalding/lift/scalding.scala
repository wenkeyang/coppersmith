package commbank.coppersmith.scalding.lift

import com.twitter.scalding._

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

  def liftJoinHl[HL <: HList, B, J : Ordering, O <: HList]
    (joined: Joined[HL, B, J, (HL,B) ])
    (a:TypedPipe[HL], b: TypedPipe[B])
    (implicit pp: Prepend.Aux[HL, B :: HNil, O])
    : TypedPipe[O] = {
    val result = a.groupBy(joined.left).join(b.groupBy(joined.right)).values.map {case (hl, r) => hl :+ r }
    result
  }
  def liftLeftJoinHl[HL <: HList, B, J : Ordering, O <: HList]
    (joined: Joined[HL, B, J, (HL, Option[B])])
    (a:TypedPipe[HL], b: TypedPipe[B])
    (implicit pp: Prepend.Aux[HL, Option[B] :: HNil, O])
      : TypedPipe[O] = {
    a.groupBy(joined.left).leftJoin(b.groupBy(joined.right)).values.map {case (hl, r) => hl :+ r}
  }

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, TypedPipe]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ) = ScaldingBoundFeatureSource(underlying, binder, filter)
}

object scalding extends ScaldingLift
