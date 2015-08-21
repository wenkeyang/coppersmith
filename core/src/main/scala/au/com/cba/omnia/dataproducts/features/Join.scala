package au.com.cba.omnia.dataproducts.features

object Join {
  sealed trait JoinType
  case object LeftOuter extends JoinType
  case object Inner extends JoinType

  trait InnerJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, Inner.type] = new IncompleteJoin[L,R, Inner.type ](Inner)
  }
  trait LeftOuterJoinableTo[L] {
    def to[R]: IncompleteJoin[L,R,  LeftOuter.type] = new IncompleteJoin[L, R, LeftOuter.type](LeftOuter)
  }

  class EmptyInnerJoinableTo[L] extends InnerJoinableTo[L]
  class EmptyLeftOuterJoinableTo[L] extends LeftOuterJoinableTo[L]

  class IncompleteJoin[L, R, JT <: JoinType](joinType: JT) {
    //Write as many of these as we need...
    def on[J : Ordering](l: L => J, r: R => J): Joined[L, R, J, JT] = Joined[L, R, J, JT](l, r)
  }

  case class Joined[L, R, J : Ordering, JT <: JoinType](left: L => J, right: R => J)

  def join[T]: InnerJoinableTo[T] = new EmptyInnerJoinableTo[T]
  def left[T]: LeftOuterJoinableTo[T] = new EmptyLeftOuterJoinableTo[T]
  def apply[T] = join[T]
}


object JoinDemo {
  case class Customer(id:Long)
  case class Account(id: Long, customerId:Long)
  case class HomeLoan(id:Long, accountId:Long, primaryCustomer: Long)

  Join[Customer]
    .to[Account].on(_.id, _.customerId)

  Join.left[Customer]
    .to[Account].on(_.id, _.customerId)
}