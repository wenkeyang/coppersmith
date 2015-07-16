package au.com.cba.omnia.dataproducts.features.examples

import au.com.cba.omnia.dataproducts.features.Feature._
import au.com.cba.omnia.dataproducts.features._
import Pivot._
import au.com.cba.omnia.dataproducts.features.example.thrift.Customer


object Example1 {
  val pivoted = pivotThrift[Customer]("namespace")
  val acct: Feature[Customer, Value.Str] = pivoted.Acct
  val cat: Feature[Customer, Value.Str] = pivoted.Cat
  val balance: Feature[Customer, Value.Integral] = pivoted.Balance

  def main(args:Array[String]) = {
    println(pivoted)
  }
}
