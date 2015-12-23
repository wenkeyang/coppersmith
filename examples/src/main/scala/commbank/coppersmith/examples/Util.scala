package commbank.coppersmith.examples

import com.twitter.scalding._,TDsl._

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.api.Splitter

import au.com.cba.omnia.maestro.core.codec.{DecodeError, DecodeOk, DecodeResult, Decode}

/**
 * Functionality common to examples. Filling in the gap between this library and the (at the moment
 * still private) etl-util
 */
object Util {
  def decodeHive[T : Decode](src: TextLineScheme, sep: String = "|") = {
    val ev = implicitly[Decode[T]]
    val results: TypedPipe[DecodeResult[T]] =
      src.map { raw => ev.decode(none="\\N", Splitter.delimited(sep).run(raw).toList) }

    // if success then fetch the rows corresponding to successful decode
    val success = results.collect {
      case DecodeOk(row) => row
    }

    // if fail then fetch the rows corresponding to failed decode and return proper error message
    val fail = results.collect {
      case DecodeError(remainder, cnt, reason) => s"${reason.toString} ${remainder.toString} \n"
    }

    (success, fail)
  }
}
