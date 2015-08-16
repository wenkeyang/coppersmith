package au.com.cba.omnia.dataproducts.features.examples

import au.com.cba.omnia.dataproducts.features.Feature._
import au.com.cba.omnia.dataproducts.features.PivotMacro._
import au.com.cba.omnia.dataproducts.features._
import au.com.cba.omnia.dataproducts.features.example.thrift.Customer
import au.com.cba.omnia.dataproducts.features.lift.memory._
import org.joda.time.DateTime


object Example1Memory {
  val pivoted = pivotThrift[Customer]("namespace", _.id, c => DateTime.parse(c.effectiveDate).getMillis())
  val pivotedAsFeatureSet: PivotFeatureSet[Customer] = pivoted
  val acct: Feature[Customer, Value.Str] = pivoted.acct
  val cat: Feature[Customer, Value.Str] = pivoted.cat
  val balance: Feature[Customer, Value.Integral] = pivoted.balance

  def main(args: Array[String]) = {
    val c1 = new Customer()
    val c2 = new Customer()

    c1.id = ""
    c1.acct = "123"
    c1.cat = "333"
    c1.subCat = "444"
    c1.balance = 100
    c1.effectiveDate = "01022001"


    c2.id = ""
    c2.acct = "124"
    c2.cat = "333"
    c2.subCat = "444"
    c2.balance = 100
    c2.effectiveDate = "01022001"

    materialise(acct)(List(c1, c2), it => println(it.value))()
  }

}

