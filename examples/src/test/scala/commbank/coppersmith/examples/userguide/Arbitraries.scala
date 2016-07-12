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

import java.util.Date
import java.text.SimpleDateFormat

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary

import commbank.coppersmith.examples.thrift.{Movie, Rating, User}

object UserGuideArbitraries {
  // Override the default implicit Arbitrary[String] (brought into scope by Arbitrary.arbString)
  // to avoid generating Customer & Account records with strings that can't be safely written to
  // a Hive Text store (due to newline or field separator characters being generated).
  implicit def arbSafeHiveTextString: Arbitrary[String] = Arbitrary(Gen.identifier)

  val genMovieId = Gen.choose(1, 10000).map(_.toString)
  val genUserId  = Gen.choose(1, 100000).map(_.toString)

  implicit def arbMovie: Arbitrary[Movie] = Arbitrary {
    def formatDate(date: Date) = new SimpleDateFormat("dd-MMM-yyyy").format(date)
    for {
      id               <- genMovieId
      title            <- arbitrary[String]
      releaseDate      <- arbitrary[Option[Date]].map(_.map(formatDate))
      videoReleaseDate <- arbitrary[Option[Date]].map(_.map(formatDate))
      url              <- arbitrary[Option[String]]
      genres           <- Gen.containerOfN[List, Boolean](19, arbitrary[Boolean])
      g                 = genres.iterator  // simple (albeit mutable) access to genre flags,
                                           // less error-prone than explicit indexing by number
    } yield Movie(
      id, title, releaseDate, videoReleaseDate, url,
      g.next, g.next, g.next, g.next, g.next, g.next, g.next, g.next, g.next, g.next,
      g.next, g.next, g.next, g.next, g.next, g.next, g.next, g.next, g.next
    )
  }

  implicit def arbRating: Arbitrary[Rating] = Arbitrary {
    for {
      userId    <- genUserId
      movieId   <- genMovieId
      rating    <- Gen.oneOf(1, 2, 3, 4, 5)
      timestamp <- arbitrary[Date].map(d => (d.getTime/1000).toString)  // seconds since epoch
    } yield Rating(userId, movieId, rating, timestamp)
  }

  implicit def arbUser: Arbitrary[User] = Arbitrary {
    for {
      id         <- genUserId
      age        <- arbitrary[Int]
      gender     <- Gen.oneOf("M", "F")
      occupation <- arbitrary[String]
      zipcode    <- arbitrary[String]
    } yield User(id, age, gender, occupation, zipcode)
  }
}
