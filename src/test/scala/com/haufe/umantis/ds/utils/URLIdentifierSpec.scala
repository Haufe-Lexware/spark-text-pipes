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

package com.haufe.umantis.ds.utils

import com.haufe.umantis.ds.tests.BaseSpec
import org.scalatest.Matchers._


class URLIdentifierSpec extends BaseSpec with URLIdentifierSpecFixture {

  "URLIdentifier" should "identify correct urls" in {
    shouldMatch.foreach(url => {
      println(s"doing $url")
      URLIdentifier.isValid(url) shouldBe true
    })
  }

  "URLIdentifier" should "identify incorrect urls" in {
    shouldNotMatch.foreach(url => {
      println(s"doing $url")
      URLIdentifier.isValid(url) shouldBe false
    })
  }
}

trait URLIdentifierSpecFixture {

  val shouldMatch = Array(
    """http://foo.com/blah_blah""",
    """http://foo.com/blah_blah/""",
    """http://foo.com/blah_blah_(wikipedia)""",
    """http://foo.com/blah_blah_(wikipedia)_(again)""",
    """http://www.example.com/wpstyle/?p=364""",
    """https://www.example.com/foo/?bar=baz&inga=42&quux""",
    """http://✪df.ws/123""",
    """http://userid:password@example.com:8080""",
    """http://userid:password@example.com:8080/""",
    """http://userid@example.com""",
    """http://userid@example.com/""",
    """http://userid@example.com:8080""",
    """http://userid@example.com:8080/""",
    """http://userid:password@example.com""",
    """http://userid:password@example.com/""",
    """http://142.42.1.1/""",
    """http://142.42.1.1:8080/""",
    """http://➡.ws/䨹""",
    """http://⌘.ws""",
    """http://⌘.ws/""",
    """http://foo.com/blah_(wikipedia)#cite-1""",
    """http://foo.com/blah_(wikipedia)_blah#cite-1""",
    """http://foo.com/unicode_(✪)_in_parens""",
    """http://foo.com/(something)?after=parens""",
    """http://☺.damowmow.com/""",
    """http://code.google.com/events/#&product=browser""",
    """http://j.mp""",
    """ftp://foo.bar/baz""",
    """http://foo.bar/?q=Test%20URL-encoded%20stuff""",
    """http://مثال.إختبار""",
    """http://例子.测试""",
    """http://उदाहरण.परीक्षा""",
    """http://-.~_!$&'()*+,;=:%40:80%2f::::::@example.com""",
    """http://1337.net""",
    """http://a.b-c.de""",
    """http://223.255.255.254"""
  )

  val shouldNotMatch = Array(
    """http://""",
    """http://.""",
    """http://..""",
    """http://../""",
    """http://?""",
    """http://??""",
    """http://??/""",
    """http://#""",
    """http://##""",
    """http://##/""",
    """http://foo.bar?q=Spaces should be encoded""",
    """//""",
    """//a""",
    """///a""",
    """///""",
    """http:///a""",
    """foo.com""",
    """rdar://1234""",
    """h://test""",
    """http:// shouldfail.com""",
    """:// should fail""",
    """http://foo.bar/foo(bar)baz quux""",
    """ftps://foo.bar/""",
    """http://-error-.invalid/""",
//    """http://a.b--c.de/""",
    """http://-a.b.co""",
    """http://a.b-.co""",
    """http://0.0.0.0""",
    """http://10.1.1.0""",
    """http://10.1.1.255""",
    """http://224.1.1.1""",
    """http://1.1.1.1.1""",
    """http://123.123.123""",
    """http://3628126748""",
    """http://.www.foo.bar/""",
//    """http://www.foo.bar./""",
    """http://.www.foo.bar./""",
    """http://10.1.1.1"""
  )
}