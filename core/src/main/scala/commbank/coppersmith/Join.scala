package commbank.coppersmith

object From {
  def apply[S](): From[S] = From(None)
}

case class From[S](filter: Option[S => Boolean] = None) extends FeatureSource[S, From[S]](filter) {
  type FS = From[S]
  def copyWithFilter(filter: Option[S => Boolean]) = copy(filter)
}

object Join {
  trait InnerJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, (L, R)] = new IncompleteJoin[L, R, (L, R)]
  }
  trait LeftOuterJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, (L, Option[R])] = new IncompleteJoin[L, R, (L, Option[R])]
  }

  class EmptyInnerJoinableTo[L] extends InnerJoinableTo[L]
  class EmptyLeftOuterJoinableTo[L] extends LeftOuterJoinableTo[L]

  class IncompleteJoin[L, R, S] {
    //Write as many of these as we need...
    def on[J : Ordering](l: L => J, r: R => J): Joined[L, R, J, S] = Joined(l, r)
  }

  case class Joined[L, R, J : Ordering, S](
    left: L => J,
    right: R => J,
    filter: Option[S => Boolean] = None
  ) extends FeatureSource[S, Joined[L, R, J, S]](None) {

    type FS = Joined[L, R, J, S]
    def copyWithFilter(filter: Option[S => Boolean]) = copy(filter = filter
    )
  }

  def join[T]: InnerJoinableTo[T] = new EmptyInnerJoinableTo[T]
  def left[T]: LeftOuterJoinableTo[T] = new EmptyLeftOuterJoinableTo[T]
  def apply[T] = join[T]
}
