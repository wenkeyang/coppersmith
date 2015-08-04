package au.com.cba.omnia.dataproducts.features.examples

import au.com.cba.omnia.dataproducts.features.Feature._
import au.com.cba.omnia.dataproducts.features.PivotMacro._
import au.com.cba.omnia.dataproducts.features._
import au.com.cba.omnia.dataproducts.features.example.thrift.Customer
import au.com.cba.omnia.dataproducts.features.lift.memory._
import au.com.cba.omnia.etl.util.ParseUtils
import au.com.cba.omnia.maestro.api.Maestro._
import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.scalding.JobStatus
import com.twitter.scalding._
import org.joda.time.DateTime

import scalaz.Scalaz._
import scalaz.{Value => _, _}

object Example1Memory {
  val pivoted = pivotThrift[Customer]("namespace", _.id, c => DateTime.parse(c.effectiveDate).getMillis())
  val pivotedAsFeatureSet:PivotFeatureSet[Customer] = pivoted
  val acct: Feature[Customer, Value.Str] = pivoted.Acct
  val cat: Feature[Customer, Value.Str] = pivoted.Cat
  val balance: Feature[Customer, Value.Integral] = pivoted.Balance



  def accountFeatureJob: Execution[JobStatus] = {
    for {
      conf          <- Execution.getConfig.map(ExampleConfig)
      inputPipe     <- Execution.from(ParseUtils.decodeHiveTextTable[Customer](
                         MultipleTextLineFiles(s"${conf.hdfsInputPath}/efft_yr_month=${conf.yearMonth}")))
      _             <- materialise(acct)(inputPipe.rows, TypedPsv(s"${conf.hivePath}/year=${conf.year}/month=${conf.month}"))
    } yield (JobFinished)
  }

  def allFeaturesJob: Execution[JobStatus] = {
    for {
      conf          <- Execution.getConfig.map(ExampleConfig)
      inputPipe     <- Execution.from(ParseUtils.decodeHiveTextTable[Customer](
        MultipleTextLineFiles(s"${conf.hdfsInputPath}/efft_yr_month=${conf.yearMonth}")))
      _             <- materialise(pivotedAsFeatureSet)(inputPipe.rows, TypedPsv(s"${conf.hivePath}/year=${conf.year}/month=${conf.month}"))
    } yield (JobFinished)
  }
}
