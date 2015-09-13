package commbank.coppersmith
import commbank.coppersmith.Feature.Value
import commbank.coppersmith.Join._
import commbank.coppersmith.Feature.Value
import shapeless._
import shapeless.ops.hlist.Prepend

import scalaz.{Ordering => _, _}, Scalaz._

import Feature.Value
import Join._
import TypeHelpers._

trait Lift[P[_]] {
  def lift[S, V <: Value](f:Feature[S,V])(s: P[S]): P[FeatureValue[V]]

  def lift[S](fs: FeatureSet[S])(s: P[S]): P[FeatureValue[_]]


  //Join stuff

  def liftJoinHl[HL <: HList, B, J : Ordering]
    (joined: Joined[HL, B, J, (HL, B) ])
    (a:P[HL], b: P[B])
    (implicit prepend: HL :+ B)
    : P[prepend.Out]

  def liftLeftJoinHl[HL <: HList, B, J : Ordering]
  (joined: Joined[HL, B, J, (HL, Option[B]) ])
  (a:P[HL], b: P[B])
  (implicit prepend: HL :+ Option[B])
  : P[prepend.Out]


  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, B) ])(a:P[A], b: P[B])(implicit functor: Functor[P]): P[(A, B)] = {
    val result = liftJoinHl(new Joined[A :: HNil, B, J, (A :: HNil, B)](
      left = (hl: A :: HNil) => joined.left(hl.head),
      right = joined.right,
      filter = None
    ))(a.map(_ :: HNil), b)

    result.map(_.tupled)
  }

  def liftLeftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, Option[B])])(a: P[A], b: P[B])(implicit functor: Functor[P]): P[(A, Option[B])] = {
    val result = liftLeftJoinHl(new Joined[A :: HNil, B, J, (A :: HNil, Option[B])](
      left = (hl: A :: HNil) => joined.left(hl.head),
      right = joined.right,
      filter = None
    ))(a.map(_ :: HNil), b)

    result.map(_.tupled)
  }

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, P]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ): BoundFeatureSource[S, P]
}
