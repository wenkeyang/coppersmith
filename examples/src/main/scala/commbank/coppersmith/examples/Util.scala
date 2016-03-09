//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

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
