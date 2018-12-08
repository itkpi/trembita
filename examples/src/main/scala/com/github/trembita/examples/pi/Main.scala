package com.github.trembita.examples.pi

import com.github.trembita.pi._
import _root_.hammock._
import _root_.hammock.jvm._
import Api._
import cats.Id
import cats.effect.{IO, Sync}
import cats.implicits._
import cats.effect.implicits._
import cats.free.Free


object Main {
  def main(args: Array[String]): Unit = {
    implicit val interpreter: Interpreter[IO] = Interpreter[IO]


    val googleKey: String = "???" // put your key here

    val res: IO[HttpResponse] = Geocoding.Api[Geocoding.Geocode].apply(googleKey, "Kyiv").exec[IO]

    val res2: Int = MyApi.Api[MyApi.Increment].apply(2)
    val res3: IO[HttpResponse] = GoogleMaps.Api[Geocoding.ReverseGeocode].apply(googleKey, Coordinates(50.4501, 30.5234)).exec[IO]

    println(res.unsafeRunSync())
    println(res2)
    println(res3.unsafeRunSync())
  }
}
