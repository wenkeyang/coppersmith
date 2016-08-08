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

import com.twitter.scalding.{TypedPsv, Config, MultipleTextLineFiles, Execution}

import scalaz.Scalaz._

import org.joda.time.DateTime

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.scalding.lift.scalding._

import commbank.coppersmith.examples.thrift._

object Example2 {
  val customerJoinAccount = Join[Customer].to[Account].on(_.acct, _.id)

  val feature = Patterns.general[(Customer, Account), FloatingPoint](
      "ns",
      "name",
      "description",
      Continuous, { case (c, a) => c._1 }, { case (c, a) => Some(a.balance) },
      None
  )

  case class ExampleConfig(config: Config) {
    val args          = config.getArgs
    val hdfsInputPath = args("input-dir")
    val queryDate =
      args.optional("query-date").cata(new DateTime(_), DateTime.now().minusMonths(1))
    val yearMonth = queryDate.toString("yyyyMM")
    val env       = args("hdfs-root")
    val hivePath  = s"${env}/view/warehouse/features/customers"
    val year      = queryDate.toString("yyyy")
    val month     = queryDate.toString("MM")
  }

  def featureJob: Execution[JobStatus] = {
    for {
      conf <- Execution.getConfig.map(ExampleConfig)
      (customers, _) <- Execution.from(
                           Util.decodeHive[Customer](MultipleTextLineFiles(
                                   s"${conf.hdfsInputPath}/cust/efft_yr_month=${conf.yearMonth}")))
      (accounts, _) <- Execution.from(
                          Util.decodeHive[Account](MultipleTextLineFiles(
                                  s"${conf.hdfsInputPath}/acct/efft_yr_month=${conf.yearMonth}")))
      outputPipe = liftJoin(customerJoinAccount)(customers, accounts)
      _ <- outputPipe.writeExecution(
              TypedPsv(s"${conf.hivePath}/year=${conf.year}/month=${conf.month}"))
    } yield (JobFinished)
  }
}
