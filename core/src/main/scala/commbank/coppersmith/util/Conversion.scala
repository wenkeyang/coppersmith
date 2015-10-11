package commbank.coppersmith.util

import shapeless._
import shapeless.ops.hlist.Tupler

/**
 * Typeclass for conversion between tuples and HLists in both directions. The
 * idea is to infer either the left or right types. Somewhere betwen `Tupler` and
 * `Generic`. The issue with `Tupler` is that it gives us H => T but not the other
 * way around. The issue with `Generic` is that while it supports both directions,
 * it doesn't infer the `T` type correctly if not already known.
 */
trait Conversion {
  type T <: Product
  type H <: HList
  def to(t: T): H
  def from(h: H): T
}


object Conversion {
  type AuxT[T0 <: Product] = Conversion {
    type T = T0
  }
  type AuxH[H0 <: HList] = Conversion {
    type H = H0
  }

  type Aux[T0 <: Product, H0 <: HList] = Conversion {
    type T = T0
    type H = H0
  }

  //TODO: Explore either auto-generating this or doing it recursively.
  //      If that fails, go up to 22 :(

  implicit def conversion1[A] = new Conversion {
    type T = Tuple1[A]
    type H = A :: HNil

    def to(t: T) = t._1 :: HNil
    def from(h: H) = Tuple1(h.head)
  }

  implicit def conversion2[A, B] = new Conversion {
    type T = (A, B)
    type H = A :: B :: HNil

    def to(t: T) = t._1 :: t. _2 :: HNil
    def from(h: H) = (h.head, h.tail.head)
  }

  implicit def conversion3[A, B, C] = new Conversion {
    type T = (A, B, C)
    type H = A :: B :: C :: HNil

    def to(t: T) = t._1 :: t. _2 :: t._3 :: HNil
    def from(h: H) = (h.head, h.tail.head, h.tail.tail.head)
  }


  implicit def conversion4[A, B, C, D] = new Conversion {
    type T = (A, B, C, D)
    type H = A :: B :: C :: D :: HNil

    def to(t: T) = t._1 :: t. _2 :: t._3 :: t._4 :: HNil
    def from(h: H) = (h.head, h.tail.head, h.tail.tail.head, h.tail.tail.tail.head)
  }

  implicit def conversion5[A, B, C, D, E] = new Conversion {
    type T = (A, B, C, D, E)
    type H = A :: B :: C :: D :: E :: HNil

    def to(t: T) = t._1 :: t. _2 :: t._3 :: t._4 :: t._5 :: HNil
    def from(h: H) = (h.head, h.tail.head, h.tail.tail.head, h.tail.tail.tail.head, h.tail.tail.tail.tail.head)
  }

}
