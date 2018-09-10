package com.haufe.umantis.ds.utils

import com.haufe.umantis.ds.tests.BaseSpec

import org.scalatest._
import Matchers._

class EmojiRemoverTransliteratorSpec extends BaseSpec {

  "EmojiRemoverTransliterator" must "remove all the Emojis" in {
    val transliterator = new EmojiRemoverTransliterator()

    // Emoji 1: bicep
    // Emoji 2: black bicep (i.e. with Fitzpatrick modifier)
    // Emoji 3: lipstick (does not support Fitzpatrick modifiers)
    val input = "Hola a todos 💪 \uD83D\uDCAA\uD83C\uDFFF hello everyone 💄"
    val expectedOutput = "Hola a todos         hello everyone   "

    transliterator.transliterate(input) shouldBe expectedOutput
  }
}
