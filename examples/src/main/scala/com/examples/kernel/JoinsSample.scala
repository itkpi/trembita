package com.examples.kernel

import cats.effect._
import cats.implicits._
import trembita._
import cats.effect.Console.io._

case class Person(personId: Long, name: String, age: Int)
case class Car(carId: Long, ownerId: Long, model: String)

object JoinsSample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val cars = Input
      .sequential[Seq]
      .create(
        Seq(
          Car(1, 1, "BMW M5"),
          Car(2, 1, "Mazda RX7"),
          Car(3, 2, "Lamborghini Galardo")
        )
      )

    val people =
      Input
        .sequential[Seq]
        .create(
          Seq(
            Person(1, "John", 34),
            Person(2, "Will", 29)
          )
        )

    val joined = cars
      .join(people)(on = _.ownerId == _.personId)
      .map { case (car, person) => s"${person.name} has ${car.model}" }
      .into(Output.vector)
      .run

    putStrLn(joined.mkString("\n")).as(ExitCode.Success)
  }
}
