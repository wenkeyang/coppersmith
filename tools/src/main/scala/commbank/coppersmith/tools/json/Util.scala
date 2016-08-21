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

package commbank.coppersmith.tools.json

import argonaut.Argonaut._
import argonaut.{JsonObject, Json, EncodeJson}
import scalaz._, Scalaz._

object Util {
  def stripNullValuesFromObjects[A](enc: EncodeJson[A])(fieldNames: String*): EncodeJson[A] = new EncodeJson[A] {
    def encode(a: A): Json = {
      val obj = enc.encode(a)
      obj.withObject { o =>
        val objMap = o.toMap
        val newFields: List[(JsonField, Json)] = o.fields.flatMap { f =>
          val v = objMap(f)
          if (v.isNull) {
            if (fieldNames.contains(f)) {
              None
            } else {
              Some(f -> jNull)
            }
          } else {
            Some(f -> v)
          }
        }
        JsonObject.from(newFields)
      }
    }
  }
}
