package com.examples.kernel

import cats.effect._
import cats.implicits._
import com.github.trembita._
import cats.effect.Console.io._

case class Person(personId: Long, name: String, age: Int)
case class Car(carId: Long, ownerId: Long, model: String)

object JoinsSample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val cars = DataPipelineT[IO, Car](
      Car(1, 1, "BMW M5"),
      Car(2, 1, "Mazda RX7"),
      Car(3, 2, "Lamborghini Galardo")
    )
    val people =
      DataPipelineT[IO, Person](Person(1, "John", 34), Person(2, "Will", 29))

    val joined = cars
      .join(people)(on = _.ownerId == _.personId)
      .map { case (car, person) => s"${person.name} has ${car.model}" }
      .eval

    joined.flatTap(vs => putStrLn(vs.mkString("\n"))).as(ExitCode.Success)
  }
}
