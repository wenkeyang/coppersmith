package commbank.coppersmith.examples

import commbank.coppersmith.Feature.Value.Str
import commbank.coppersmith.Feature._
import commbank.coppersmith.PivotMacro._
import commbank.coppersmith.{lift => _, _}
import commbank.coppersmith.example.thrift.Customer
import commbank.coppersmith.lift.memory._
import org.joda.time.DateTime


object Example1Memory {
  val pivoted = pivotThrift[Customer]("namespace", _.id, c => DateTime.parse(c.effectiveDate).getMillis())
  val pivotedAsFeatureSet: PivotFeatureSet[Customer] = pivoted
  val acct: Feature[Customer, Value.Str] = pivoted.acct
  val cat: Feature[Customer, Value.Str] = pivoted.cat
  val balance: Feature[Customer, Value.Integral] = pivoted.balance

  def main(args: Array[String]) = {
    val c1 = Customer(
      id = "",
      acct = "123",
      cat = "333",
      subCat = "444",
      balance = 100,
      effectiveDate = "01022001",
      dob = "10091988"
    )

    val c2 = Customer(
      id = "",
      acct = "124",
      cat = "333",
      subCat = "444",
      balance = 100,
      effectiveDate = "01022001",
      dob = "11101999"
    )

    val result: List[FeatureValue[Str]] = lift(acct)(List(c1, c2))
  }

}

