package au.com.cba.omnia.dataproducts.features.examples

import au.com.cba.omnia.dataproducts.features.Feature._
import au.com.cba.omnia.dataproducts.features._
import PivotMacro._
import au.com.cba.omnia.dataproducts.features.example.thrift.Customer
import au.com.cba.omnia.dataproducts.features.scalding._
import au.com.cba.omnia.etl.util.{ParseUtils, SimpleMaestroJob}
import au.com.cba.omnia.maestro.scalding.JobStatus
import com.twitter.scalding.{Config, MultipleTextLineFiles, Execution}
import org.joda.time.DateTime

import au.com.cba.omnia.maestro.api._, Maestro._

import scalaz.{Value => _, _}
import Scalaz._


object Example1 {
  val pivoted = pivotThrift[Customer]("namespace", _.id, c => DateTime.parse(c.effectiveDate).getMillis())
  val pivotedAsFeatureSet:PivotFeatureSet[Customer] = pivoted
  val acct: Feature[Customer, Value.Str] = pivoted.Acct
  val cat: Feature[Customer, Value.Str] = pivoted.Cat
  val balance: Feature[Customer, Value.Integral] = pivoted.Balance

  case class ExampleConfig(config:Config) {
    val args          = config.getArgs
    val hdfsInputPath = args("input-dir")
    val queryDate     = args.optional("query-date").cata(new DateTime(_),DateTime.now().minusMonths(1))
    val yearMonth     = queryDate.toString("yyyyMM")
  }

  def job:Execution[JobStatus] = {
    materialise(acct) _
      for {
      conf          <- Execution.getConfig.map(ExampleConfig)
      inputPipe     <- Execution.from(ParseUtils.decodeHiveTextTable[Customer](
        MultipleTextLineFiles(s"${conf.hdfsInputPath}/efft_yr_month=${conf.yearMonth}")))
    } yield (???)
  }

  def main(args:Array[String]) = {

  }
}
