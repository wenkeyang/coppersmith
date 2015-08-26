#@namespace scala commbank.coppersmith.test.thrift

struct Customer {
  1 : required string id
  2 : required string name
  3 : required i32    age
  4 : required double height
  5 : required i64    time
}

// Type with some optional fields
struct Account {
  1 : required string id
  2 : required string customer_id
  3 : required double balance
  4 :          string name
  5 :          i32    age
  6 :          double min_balance
  7 : required i64    time
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