package suggestions

import language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Try, Success, Failure }
import rx.lang.scala._
import org.scalatest._
import gui._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WikipediaApiTest extends FunSuite {

  object mockApi extends WikipediaApi {
    def wikipediaSuggestion(term: String) = Future {
      if (term.head.isLetter) {
        for (suffix <- List(" (Computer Scientist)", " (Footballer)")) yield term + suffix
      } else {
        List(term)
      }
    }
    def wikipediaPage(term: String) = Future {
      "Title: " + term
    }
  }

  import mockApi._

  test("WikipediaApi should make the stream valid using sanitized") {
    val notvalid = Observable("erik", "erik meijer", "martin")
    val valid = notvalid.sanitized

    var count = 0
    var completed = false

    val sub = valid.subscribe(
      term => {
        assert(term.forall(_ != ' '))
        count += 1
      },
      t => assert(false, s"stream error $t"),
      () => completed = true)
    assert(completed && count == 3, "completed: " + completed + ", event count: " + count)
  }

  test("WikipediaApi should correctly use concatRecovered") {
    val requests = Observable(1, 2, 3)
    val remoteComputation = (n: Int) => Observable(0 to n)
    val responses = requests concatRecovered remoteComputation
    val sum = responses.foldLeft(0) { (acc, tn) =>

      tn match {
        case Success(n) => acc + n
        case Failure(t) => throw t
      }
    }
    var total = -1
    val sub = sum.subscribe {
      s => total = s
    }
    assert(total == (1 + 1 + 2 + 1 + 2 + 3), s"Sum: $total")
  }

  test("sum of Observable(1, 2, 3, 4, 5) should be 15") {
    val req = Observable(1, 2, 3, 4, 5)
    val sum = req.reduce(_ + _)

    var total = -1
    val sub = sum.subscribe(total = _)

    assert(total == 15, s"Sum: $total")
  }

  test("sum of recovered Observable(1, 2, 3, 4, 5) should be 15 too") {
    val req = Observable(1, 2, 3, 4, 5).recovered
    val sum = req.foldLeft(0) { (acc, tn) =>
      tn match {
        case Success(n) => acc + n
        case Failure(t) => throw t
      }
    }

    var total = -1
    var completed = false
    val sub = sum.subscribe(
      total = _,
      t => assert(false, s"stream error $t"),
      () => completed = true)

    assert(completed)
    assert(total == 15, s"Sum: $total")
  }

  test("recovered Observable(1, error, 3) should be Observable(Success(1), Failure(error))") {
    val req: Observable[Int] = Observable(1) ++ Observable(new Exception("MyException")) ++ Observable(3)
    val response: Observable[Try[Int]] = req.recovered
    val l = response.toBlockingObservable.toList

    assert(l.size == 2)
    assert(l.head == Success(1))
    val Failure(e) = l.last
    assert(e.getMessage() == "MyException")
  }
  
  test("concatRecovered is recover and then concat") {
    val req: Observable[Int] = Observable(1, 2, 3)
    val response: Observable[Try[Int]] = 
      req.concatRecovered(num => if (num != 2) Observable(num) else Observable(new Exception("MyException")))
    val l = response.toBlockingObservable.toList
    
    assert(l.size == 3)
    assert(l.head == Success(1))
    assert(l.last == Success(3))
    val Failure(e) = l(1)
    assert(e.getMessage() == "MyException")
  }

}