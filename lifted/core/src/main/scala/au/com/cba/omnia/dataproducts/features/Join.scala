package au.com.cba.omnia.dataproducts.features

object Join {
  type JoinCondition = Boolean

  trait JoinableTo[L] {
    def to[R]: IncompleteJoin[L,R] = new IncompleteJoin[L,R]
  }

  class EmptyJoinableTo[L] extends JoinableTo[L]

  class IncompleteJoin[L,R] {
    //Write as many of these as we need...
    def on[J : Ordering](l: L => J, r: R => J): Joined[L, R, J] = Joined[L, R, J](l,r)

//    TODO: Fix multi-arity
//    def on[P1,P2](cond: (P1, P2, R) => JoinCondition)(implicit e: (P1,P2)=:=L): Joined[(P1, P2), R] =
//      Joined((t1: (P1, P2), p3:R) => cond(t1._1, t1._2, p3))
//
//    def on[P1,P2,P3](cond: (P1, P2, P3, R) => JoinCondition)(implicit e: ((P1, P2), P3) =:= L): Joined[(P1, P2, P3), R] =
//      Joined((t1: (P1, P2, P3), r:R) => cond(t1._1, t1._2, t1._3, r))
//
//    def on[P1,P2,P3,P4](cond: (P1, P2, P3, P4,  R) => JoinCondition)(implicit e: (((P1, P2), P3), P4) =:= L): Joined[(P1, P2, P3, P4), R] =
//      Joined((t1: (P1, P2, P3, P4), r:R) => cond(t1._1, t1._2, t1._3, t1._4, r))
  }

  case class Joined[L, R, J : Ordering](left: L => J, right: R => J) extends JoinableTo[(L,R)]

  def join[T]:JoinableTo[T] = new EmptyJoinableTo[T]
  def apply[T] = join[T]
}


object JoinDemo {
  import Join._
  import scalaz._, Scalaz._

  case class Customer(id:Long)
  case class Account(id: Long, customerId:Long)
  case class HomeLoan(id:Long, accountId:Long, primaryCustomer: Long)

  join[Customer]
    .to[Account].on(_.id, _.customerId)
//    .to[HomeLoan].on((customer:Customer, acct:Account, loan:HomeLoan) => loan.accountId === acct.id)
//    .to[Customer].on((customer:Customer, acct:Account, loan:HomeLoan, primaryCustomer:Customer) => loan.primaryCustomer === primaryCustomer.id)

}