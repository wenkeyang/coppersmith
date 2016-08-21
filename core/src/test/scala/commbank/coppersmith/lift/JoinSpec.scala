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

package commbank.coppersmith
package lift

//Really just for testing the compilation of joins
class JoinSpec {
  case class A(id:Int)
  case class B(id:Int, str:String)
  case class C(id:String)
  case class D(id: Int, bId: Int)

  val fourWayInner = Join.multiway[A].inner[B].on((a: A)                    => a.id,  (b: B) => b.id)
                                     .left [C].on((a: A, b:B)               => b.str, (c: C) => c.id)
                                     .inner[D].on((a: A, b:B, c:Option[C])  => b.id,  (d: D) => d.bId)

}
