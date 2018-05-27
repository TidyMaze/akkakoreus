import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

case class Article(id: Int,
                   author: String,
                   publishedAt: Date,
                   title: String,
                   description: String)

object Client {
  val MaxPage = 20

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher


    def body(r: HttpEntity.Strict): String = r.data.utf8String
    val formatter = new SimpleDateFormat("'le 'd/M/yyy HH:mm:ss")
    def parseDate(s: String) = formatter.parse(s)

    def getItems: Document => List[Element] = _.select("div.item").listIterator().asScala.toList

    def log[A]: A => A = a => {
      println(a)
      a
    }

    def itemElementToArticle(el: Element): Article = {
      val dateRaw = el.selectFirst("span.itemPostDate").text()
      try {
        Article(
          el.selectFirst("h1 > a:nth-child(2)").attr("href")
            .replaceFirst("https://www.koreus.com/modules/news/article", "")
            .replaceFirst(".html", "").toInt,
          el.selectFirst("span.itemPoster").text()
            .replaceFirst("^Posté par ", ""),
          parseDate(dateRaw),
          el.selectFirst("h1.itemTitle").text(),
          el.selectFirst("p.itemText").text()
        )
      }catch {
        case e:Throwable =>
          println(s"DATE RAW: -$dateRaw-")
          throw e
      }
    }

    def getArticles(page: Int): Future[Seq[Article]] = {
      Http().singleRequest(HttpRequest(uri = s"https://www.koreus.com/modules/news/actualiteinsolite${(page-1) * 10}.html"))
        .flatMap(_.entity.toStrict(2 second))
        .map(body)
        .map(Jsoup.parse)
        .map(getItems)
        .map(_.map(itemElementToArticle))
    }

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -3)
    cal.clear(Calendar.HOUR)
    cal.clear(Calendar.HOUR_OF_DAY)
    cal.clear(Calendar.MINUTE)
    cal.clear(Calendar.SECOND)
    cal.clear(Calendar.MILLISECOND)
    val fromDate = cal.getTime

    println("FromDate:"+fromDate)

    val (fStream: Future[Done], fSink: Future[Done]) = Source(1 to MaxPage)
      .mapAsync(1)(getArticles)
      .mapConcat(identity)
      .takeWhile(article => article.publishedAt.after(fromDate))
      .map(log)
      .watchTermination()(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    fStream
      .flatMap(_ => Future {
          Thread.sleep(100)
        })
      .recover{
        case e => Console.err.println(e)
      }
      .onComplete(tStream => {
        tStream match {
          case Success(_) => println("Stream OK")
          case Failure(ex) =>
            println("Stream KO: ")
            throw ex
        }
        Http().shutdownAllConnectionPools().onComplete(tPool => {
          tPool match {
            case Success(_) => println("Stop pool OK")
            case Failure(ex) =>
              println("Pool KO: ")
              throw ex
          }
          system.terminate().onComplete {
            case Success(_) => println("System terminate OK")
            case Failure(ex) => throw ex
          }
        })
      })
  }
}
