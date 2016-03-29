import java.util.Locale
import com.twitter.algebird.Aggregator
import com.twitter.scalding._
import commbank.coppersmith.examples.thrift.Movie
import org.joda.time.format._
import org.joda.time._

import scala.util.Try

object foo {
  new DateTime("01-01-1997")
  val format = DateTimeFormat.forPattern("dd-MMM-yyyy").withLocale(Locale.ENGLISH)
  val defaultYear = 1970
  parse.parseDateTime("01-Jan-1995")
  val movie = Movie()
  Try(format.parseDateTime(movie.releaseDate).getYear).getOrElse(defaultYear)

}
//case class Cust(followers: Long, uid: Int)
//
//val allList = List(Cust(1000000, 1), Cust(1000, 2), Cust(1000000,3))
//
//val all = TypedPipe.from(allList)
//
//val topCust = all.collect { case Cust(followers, uid) if followers >= 1000000 => uid }
//
//val top = topCust.map(Set(_)).sum
//
//val allClickers = TypedPipe.from(List(1,2,4))
//
//val topClickers = allClickers.filterWithValue(top) { (clicker, optSet) =>
//  optSet.get.contains(clicker)
//}

val directorPattern = "^([^\t]+)\t+([^\t]+)$".r
val filmPattern = "^\t+([^\t]+)$".r

val x: Option[Int] = None



val dirPipe = TypedPipe.from(TextLine("directors.list"))
val filteredPipe = dirPipe.filter((line: String) => line.contains("\t") && line.matches(".*\\([\\d\\?]{4}.*"))
val directors = filteredPipe.groupAll.foldLeft(List[(String, String)]()) { (acc: List[(String, String)], line: String) =>
  def updateHead(director: String, movies: List[(String, String)]): List[(String, String)] = {
    movies.head match {
      case ("", movie) => (director, movie) :: updateHead(director, movies.tail)
      case _ => movies
    }
  }

  line match {
    case directorPattern(director, film) => (director, film) :: updateHead(director, acc)
    case filmPattern(film) => ("", film) :: acc
    case _ => acc
  }
}.values.flatten

val sizes = directors.aggregate(Aggregator.size)