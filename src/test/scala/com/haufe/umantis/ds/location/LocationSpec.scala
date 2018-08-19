/**
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <https://www.gnu.org/licenses/>.
  */

package com.haufe.umantis.ds.location

import org.scalactic._
import org.scalatest._
import Matchers._
import com.haufe.umantis.ds.tests.BaseSpec


class Place(val name: String, val country: Option[String]) {
  def coords[T <: Location](geo: T): GeoCoordinates = {
    geo.getCoordinates(name, country)
  }
}
object Place {
  def apply(name: String, country: String = ""): Place = {
    new Place(name, if (country != "") Some(country) else None)
  }
}


class LocationSpec extends BaseSpec {
  def geo[T <: Location]: Location = Location

  def dist(p1: Place, p2: Place): Float = {
    val loc1 = geo.getCoordinates(p1.name, p1.country)
    val loc2 = geo.getCoordinates(p2.name, p2.country)
    geo.getDistance(loc1, loc2)
  }

  implicit val distanceEquality: Equality[Float] =
    TolerantNumerics.tolerantFloatEquality(0.1f)

  val locA = GeoCoordinates(1.0, 2.0)
  val locB = GeoCoordinates(4.3, 7.8)

  "A distance" must "be 0 if between same locations" in {
    val distance = Location.getDistance(locA, locA)
    distance shouldBe 0f
  }

  it must "be positive if between two different locations" in {
    val distance = Location.getDistance(locA, locB)
    distance should be > 0f
  }

  it must "be commutative" in {
    val distance = Location.getDistance(locA, locB)
    val reverseDistance = Location.getDistance(locB, locA)
    distance shouldEqual reverseDistance
  }

  it must "be half the Earth Circumference if between antipodes" in {
    val antipodalDistance = 6371 * Math.PI

    val zero = GeoCoordinates(0, 0)
    val zeroAntipode = GeoCoordinates(0.0, 180.0)
    val distance = Location.getDistance(zero, zeroAntipode)
    distance === antipodalDistance

    val northPole = GeoCoordinates(90, 0)
    val southPole = GeoCoordinates(-90, 0)
    Location.getDistance(northPole, southPole) === antipodalDistance
  }

  "findPlace" must "be None if a method of Location base class" in {
    val result = Location.findPlace("AnyPlace, Nowhere")
    result shouldBe None
  }

  "A country code" must "match its country names in English" in {
    Location.countryCodeToName(Some("us")).value shouldBe "United States of America"
    Location.countryCodeToName(Some("ES")).value shouldBe "Spain"
    Location.countryCodeToName(Some("XX")) shouldBe None
    Location.countryCodeToName(None) shouldBe None
  }

  "getCoordinates" must "return (0, 0) if place is not found" in {
    val coords = Location.getCoordinates("non existing place")
    coords shouldBe GeoCoordinates(0, 0)
  }
}
