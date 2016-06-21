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

package commbank.coppersmith.scalding

import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api._, Maestro._

import commbank.coppersmith.{FeatureValue, Feature}, Feature._, Value._
import commbank.coppersmith.thrift.Eavt

object EavtText {
  val eavtByDay = DerivedSinkPartition[Eavt, (String, String, String)](
    HivePartition.byDay(Fields[Eavt].Time, "yyyy-MM-dd")
  )

  implicit object EavtEnc extends FeatureValueEnc[Eavt] {
    def encode(fvt: (FeatureValue[_], Time)): Eavt = fvt match {
      case (fv, time) =>
        val featureValue = (fv.value match {
          case Integral(v) => v.map(_.toString)
          case Decimal(v) => v.map(_.toString)
          case FloatingPoint(v) => v.map(_.toString)
          case Str(v) => v
          case DateV(v) => v.map(_.toString)
          case TimeV(v) => v.map(_.toString)
        }).getOrElse(HiveTextSink.NullValue)

        val featureTime = new DateTime(time).toString("yyyy-MM-dd")
        Eavt(fv.entity, fv.name, featureValue, featureTime)
    }
  }
}