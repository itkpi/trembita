package com.datarootlabs.trembita.examples.pi

import com.datarootlabs.trembita.collections._
import shapeless.nat._
import cats.free.Free
import com.datarootlabs.trembita.collections.SizedAtLeast
import hammock._
import com.datarootlabs.trembita.pi._
import Api._
import com.datarootlabs.trembita.ql._

case class Coordinates(latitude: Double, longidute: Double)

object Geocoding {
  def geocode(apiKey: String, address: String): Free[HttpF, HttpResponse] = {
    val uri = uri"https://maps.googleapis.com/maps/api/geocode/json" ? ("address" -> address & "key" -> apiKey)
    Hammock.request(Method.GET, uri, Map.empty)
  }

  def reverseGeocode(apiKey: String, coordinates: Coordinates): Free[HttpF, HttpResponse] = {
    val uri =
      uri"https://maps.googleapis.com/maps/api/geocode/json" ? ("latlng" -> s"${coordinates.latitude.toString},${coordinates.longidute.toString}" & "key" -> apiKey)

    Hammock.request(Method.GET, uri, Map.empty)
  }

  trait Geocode
  trait ReverseGeocode
  type GeocodeFunc = ((String, String) => HttpRequestF) :@ Geocode
  type ReverseGeocodeFunc = ((String, Coordinates) => HttpRequestF) :@ ReverseGeocode

  type Api = GeocodeFunc |:: ReverseGeocodeFunc |:: ANil
  val Api: Api = (geocode _).:@[Geocode] |:: (reverseGeocode _).:@[ReverseGeocode] |:: ANil
}


object Roads {
  type Path = SizedAtLeast[Coordinates, _2, List]

  def snapToRoads(apiKey: String, path: Path, interpolate: Boolean): Free[HttpF, HttpResponse] = {
    val points = path.unsized.map { case Coordinates(lat, lng) => s"$lat,$lng" }.mkString("|")
    val uri =
      uri"https://roads.googleapis.com/v1/snapToRoads" ? ("path" -> points & "interpolate" -> interpolate.toString & "key" -> apiKey)

    Hammock.request(Method.GET, uri, Map.empty)
  }

  def nearestRoad(apiKey: String, path: Path): HttpRequestF = {
    val points = path.unsized.map { case Coordinates(lat, lng) => s"$lat,$lng" }.mkString("|")
    val uri =
      uri"https://roads.googleapis.com/v1/nearestRoads" ? ("points" -> points & "key" -> apiKey)

    Hammock.request(Method.GET, uri, Map.empty)
  }

  trait SnapToRoads
  trait NearestRoads

  type Api =
    (((String, Path, Boolean) => HttpRequestF) :@ SnapToRoads) |::
      (((String, Path) => HttpRequestF) :@ NearestRoads) |::
      ANil

  val Api: Api = (snapToRoads _).:@[SnapToRoads] |:: (nearestRoad _).:@[NearestRoads] |:: ANil
}

object GoogleMaps {
  type Api = Geocoding.Api +|+ Roads.Api
  val Api: Api = Geocoding.Api +|+ Roads.Api
}

object MyApi {
  trait Increment
  trait ToStr

  val increment: Int => Int    = _ + 1
  val toStr    : Int => String = p => s"as string: $p"

  type Api = ((Int => Int) :@ Increment) |:: ((Int => String) :@ ToStr) |:: GoogleMaps.Api
  val Api: Api = increment.:@[Increment] |:: toStr.:@[ToStr] |:: GoogleMaps.Api
}