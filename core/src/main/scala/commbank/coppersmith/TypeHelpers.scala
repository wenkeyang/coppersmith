package commbank.coppersmith

import shapeless._
import shapeless.ops.hlist.Prepend

object TypeHelpers {
  type :+ [HL <: HList, A] = Prepend[HL, A :: HNil]
}
