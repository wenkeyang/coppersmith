package commbank.coppersmith

import com.twitter.scalding.TypedPipe

package object scalding {
  implicit val framework: Lift[TypedPipe] = scalding.lift.scalding
}
