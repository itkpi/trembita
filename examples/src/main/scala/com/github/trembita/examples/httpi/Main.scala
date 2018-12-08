package com.github.trembita.examples.httpi

import com.github.trembita.examples.pi._
import com.github.trembita.ql._
import com.github.trembita.httpi._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.github.trembita.pi.Api._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

object Main {
  trait number
  val func: Inc = MyApi.increment.:@[MyApi.Increment]
  type Inc = (Int => Int) :@ MyApi.Increment


  case class Person(name: String, age: Int)
  def growUp(person: Person): String = person.copy(age = person.age + 1).toString

  trait grow
  type Grow = (Person => String) :@ grow

  type Api = Inc |:: Grow |:: ANil

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()
    implicit val grow = Routable.get[Grow]
    implicit val inc = Routable.auto[Inc]

    val route = Routable[Api].apply(func |:: (growUp _).:@[grow] |:: ANil)

    val bind = Http().bindAndHandle(route, "0.0.0.0", 9090)

    scala.io.StdIn.readLine("Press to exit")
    mat.shutdown()
    Await.result(for {
      _ <- bind.map(_.unbind())
      _ <- system.terminate()
    } yield (), Duration.Inf)
    sys.exit(1)
  }

}
