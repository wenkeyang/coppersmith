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

#@namespace scala commbank.coppersmith.test.thrift

struct Customer {
  1 : required string id
  2 : required string name
  3 : required i32    age
  4 : required double height
  5 : optional double credit
  6 : required i64    time
}

// Type with some optional fields
struct Account {
  1 : required string id
  2 : required string customer_id
  3 : required double balance
  4 : required string balance_big_decimal
  5 : optional string name
  6 : optional i32    age
  7 : optional double min_balance
  8 : required i64    time
}

//for testing joins
struct C {
  1 : required i32 id
  2: required string value
}

struct D {
  1 : required i32 id
  2: required string value
}
