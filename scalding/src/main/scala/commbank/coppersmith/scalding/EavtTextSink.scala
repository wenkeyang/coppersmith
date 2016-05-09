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

import commbank.coppersmith.scalding.HiveSupport.{FailJob, DelimiterConflictStrategy}
import org.apache.hadoop.fs.Path
import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api._, Maestro._

import commbank.coppersmith.{FeatureValue, Feature}, Feature._, Value._
import commbank.coppersmith.thrift.Eavt

object EavtTextSink {
  import TextSink._

  val defaultPartition = DerivedSinkPartition[Eavt, (String, String, String)](
    HivePartition.byDay(Fields[Eavt].Time, "yyyy-MM-dd")
  )

  type EavtTextSink = TextSink[Eavt]

  implicit object EavtEnc extends FeatureValueEnc[Eavt] {
    def encode(fvt: (FeatureValue[_], Time)): Eavt = fvt match {
      case (fv, time) =>
        val featureValue = (fv.value match {
          case Integral(v) => v.map(_.toString)
          case Decimal(v) => v.map(_.toString)
          case Str(v) => v
        }).getOrElse(TextSink.NullValue)

        // TODO: Does time format need to be configurable?
        val featureTime = new DateTime(time).toString("yyyy-MM-dd")
        Eavt(fv.entity, fv.name, featureValue, featureTime)
    }
  }

  def configure(dbPrefix:  String,
                 dbRoot:    Path,
                 tableName: TableName,
                 partition: SinkPartition[Eavt] = defaultPartition,
                 group:     Option[String] = None,
                 dcs:       DelimiterConflictStrategy[Eavt] = FailJob[Eavt]()): EavtTextSink = {
    TextSink.configure(dbPrefix, dbRoot, tableName, partition, group, dcs)
  }

}