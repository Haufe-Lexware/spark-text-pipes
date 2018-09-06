package com.haufe.umantis.ds.utils

import com.ibm.icu.text.{Replaceable, Transliterator, UnicodeFilter}

class EmojiRemoverTransliterator (val ID: String, val filter: UnicodeFilter)
  extends Transliterator(ID, filter) {

  def this() {
    this("Emoji-Remover", null)
  }

  override protected def handleTransliterate(
                                              text: Replaceable,
                                              pos: Transliterator.Position,
                                              incremental: Boolean)
  : Unit = {

    val emojis = new EmojiParserExtended().getAllUnicodeCandidates(text.toString)

    emojis.foreach(e => {
      // if no Fitzpatrick, .getFitzpatrickEndIndex == .getEmojiEndIndex
      val length = e.getFitzpatrickEndIndex - e.getEmojiStartIndex

      // we replace with a number of spaces equal to the Emoji + Fitzpatrick length
      text.replace(e.getEmojiStartIndex, e.getFitzpatrickEndIndex, " " * length)
    })

    pos.start = pos.limit
  }
}

object EmojiRemoverTransliterator {
  Transliterator.registerInstance(new EmojiRemoverTransliterator())
}
