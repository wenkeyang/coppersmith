package commbank.coppersmith

import commbank.coppersmith.Join.JoinedHl
import shapeless._
import shapeless.ops.hlist._

import scalaz.{Ordering => _, _}

import Feature.Value

trait Lift[P[_]] {
  def lift[S, V <: Value](f:Feature[S,V])(s: P[S]): P[FeatureValue[V]]

  def lift[S](fs: FeatureSet[S])(s: P[S]): P[FeatureValue[_]]


  //Join stuff

  def liftJoinHl[LeftSides <: HList, RightSide, J : Ordering, Out <: HList, PrevJoins <: HList]
    (joined: JoinedHl[LeftSides, RightSide, RightSide, PrevJoins, J, Out] )
    (a:P[LeftSides], b: P[RightSide])
    (implicit pp: Prepend.Aux[LeftSides, RightSide :: HNil, Out])
    : P[Out]

  def liftLeftJoinHl[LeftSides <: HList,
    RightFlat,
    J : Ordering,
    Out <: HList,
    PrevJoins <: HList]
  (joined: JoinedHl[LeftSides, Option[RightFlat], RightFlat, PrevJoins, J, Out])
  (a:P[LeftSides], b: P[RightFlat])
  (implicit pp: Prepend.Aux[LeftSides, Option[RightFlat] :: HNil, Out])
  : P[Out]

  //two is a special case, a little easier to do than the general case
  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, B) ])(a:P[A], b: P[B])(implicit functor: Functor[P]): P[(A, B)] = {
    ???
  }

  //as above
  def liftLeftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, Option[B])])(a: P[A], b: P[B])(implicit functor: Functor[P]): P[(A, Option[B])] = {
   ???
  }




  def liftMultiwayJoin[LeftSides <: HList, RightSide, FlatRight, J : Ordering, Out <: HList, POut <: HList, POutTuple <: Product, OutTuple <: Product, PrevJoins <: HList]
    (joined: JoinedHl[LeftSides, RightSide, FlatRight, PrevJoins, J, Out])
    (t: POutTuple)
    (implicit prepend:      Prepend.Aux[LeftSides, RightSide :: HNil, Out],
              mapper:       Mapped.Aux[Out, P, POut],
              pGen:         Generic.Aux[POut, POutTuple],
              outGen:       Tupler.Aux[Out, OutTuple],
              consEvidence: IsHCons[POut] //because empty HLists don't make sense in this context
      ): P[OutTuple] = {
    val pipeHList: POut = pGen.from(t)
//    pipeHList.tail.foldRight(pipeHList.head :: HNil)(???)
    ???
  }



  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, P]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ): BoundFeatureSource[S, P]



}
