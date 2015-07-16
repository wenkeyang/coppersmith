package au.com.cba.omnia.dataproducts.features.examples

import au.com.cba.omnia.dataproducts.features.Feature._
import au.com.cba.omnia.dataproducts.features._
import PivotMacro._
import au.com.cba.omnia.dataproducts.features.example.thrift.Customer


object Example1 {
  val pivoted = pivotThrift[Customer]("namespace", _.id, _ => System.currentTimeMillis())
  val pivotedAsFeatureSet:PivotFeatureSet[Customer] = pivoted
  val acct: Feature[Customer, Value.Str] = pivoted.Acct
  val cat: Feature[Customer, Value.Str] = pivoted.Cat
  val balance: Feature[Customer, Value.Integral] = pivoted.Balance

}
