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

import dispatch._
import Defaults._
import com.haufe.umantis.ds.utils.ConfigGetter
import io.circe.parser.decode
import io.circe.generic.auto._
import io.circe.syntax._


object Text2GeolocationClient extends Location with ConfigGetter {

  val serviceUrl: String = getConfig("/text2geolocation.conf")
    .getString("location.url")

  case class Place(name: String, country: Option[String] = None)

  override def findPlace(place: String,
                         country: Option[String] = None): Option[GeoLocation] = {

    val placeAndCountry = Place(place, country).asJson.toString

    val svc = url(serviceUrl).POST.setBody(placeAndCountry)
    val coordinates = Http.default(svc OK as.String)

    decode[GeoCoordinates](coordinates())
      match {
        case Right(c) => Some(GeoLocation(c.latitude, c.longitude, ""))
        case _ => Some(GeoLocation(0.0, 0.0, ""))
      }
  }
}
