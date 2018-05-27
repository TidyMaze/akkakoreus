import java.io.File
import java.nio.file.Path
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneOffset}

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpRequest}
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.util.ByteString
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
                   publishedAt: Instant,
                   title: String,
                   description: String)

object Client {
  val MaxPage = Int.MaxValue

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher


    def body(r: HttpEntity.Strict): String = r.data.utf8String

    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("'le 'd/M/yyyy H:mm:ss")


    def parseDate(dateRaw: String) = try {
      LocalDateTime.parse(dateRaw, formatter).toInstant(ZoneOffset.UTC)
    } catch {
      case e: Throwable =>
        Console.err.println(s"DATE RAW: -$dateRaw- ex = ${e.getMessage} again : ${formatter.parse(dateRaw)}")
        throw e
    }

    def getItems: Document => List[Element] = _.select("div.item").listIterator().asScala.toList

    def log[A]: A => A = a => {
      println(a)
      a
    }

    def itemElementToArticle(el: Element): Article = {
      try {
        Article(
          el.selectFirst("h1 > a:nth-child(2)").attr("href")
            .replaceFirst("https://www.koreus.com/modules/news/article", "")
            .replaceFirst(".html", "").toInt,
          el.selectFirst("span.itemPoster").text()
            .replaceFirst("^Posté par ", ""),
          parseDate(el.selectFirst("span.itemPostDate").text()),
          el.selectFirst("h1.itemTitle").text(),
          el.selectFirst("p.itemText").text()
        )
      } catch{
        case e: Throwable =>
          Console.err.println("Error when converting element :\n"+el)
          throw e
      }
    }

    def getArticles(page: Int): Future[Seq[Article]] = {
      Http().singleRequest(HttpRequest(uri = s"https://www.koreus.com/modules/news/actualiteinsolite${(page - 1) * 10}.html"))
        .flatMap(_.entity.toStrict(2 second))
        .map(body)
        .map(Jsoup.parse)
        .map(getItems)
        .map(_.map(itemElementToArticle))
    }

    val today = Instant.now()
    val fromDate = today.plus(-100, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS)

    println("FromDate:" + formatter.format(fromDate.atOffset(ZoneOffset.UTC)))

    val writeSink = FileIO.toPath(new File("out.txt").toPath)

    val (fStream: Future[Done], fSink: Future[IOResult]) = Source(1 to MaxPage)
      .mapAsync(10)(getArticles)
      .throttle(2, 1.second)
      .mapConcat(identity)
      .takeWhile(article => article.publishedAt.isAfter(fromDate))
      .map(log)
      .map(a => ByteString(a.toString+"\n"))
      .watchTermination()(Keep.right)
      .toMat(writeSink)(Keep.both)
      .run()

    fStream
      .flatMap(_ => Future {
        Thread.sleep(1000)
      })
      .recover {
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
