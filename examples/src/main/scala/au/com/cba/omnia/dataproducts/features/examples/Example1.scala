package au.com.cba.omnia.dataproducts.features.examples

import au.com.cba.omnia.dataproducts.features._
import Pivot._
import au.com.cba.omnia.dataproducts.features.example.thrift.Customer


object Example1 {
  val acct: Feature[Customer] = pivotThrift[Customer]("namespace").Acct
  val cat: Feature[Customer] = pivotThrift[Customer]("namespace").Cat
}
