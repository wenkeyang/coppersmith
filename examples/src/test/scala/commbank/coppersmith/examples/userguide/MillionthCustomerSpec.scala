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

package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api.JobFinished

import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

object MillionthCustomerSpec extends ThermometerHiveSpec { def is = s2"""
  MillionthCustomerFeaturesJob must return expected values  $test  ${tag("slow")}
"""
  def test = {
    executesSuccessfully(MillionthCustomerFeaturesJob.job) must_== JobFinished
    expectations { context =>
      context.lines(new Path(s"$dir/user/dev/view/warehouse/features/customers/year=2015/month=08/day=29/*")).toSet must_== Set(
        "1000000|CUST_MILLIONTH|Y|2015-08-29",
        "2000000|CUST_MILLIONTH|Y|2015-08-29",
        "3000000|CUST_MILLIONTH|Y|2015-08-29",
        "4000000|CUST_MILLIONTH|Y|2015-08-29",
        "5000000|CUST_MILLIONTH|Y|2015-08-29",
        "6000000|CUST_MILLIONTH|Y|2015-08-29",
        "7000000|CUST_MILLIONTH|Y|2015-08-29",
        "8000000|CUST_MILLIONTH|Y|2015-08-29",
        "9000000|CUST_MILLIONTH|Y|2015-08-29",
        "10000000|CUST_MILLIONTH|Y|2015-08-29"
      )
    }
  }
}
