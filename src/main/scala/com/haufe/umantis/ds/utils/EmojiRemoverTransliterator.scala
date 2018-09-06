package com.haufe.umantis.ds.utils

import com.ibm.icu.text.{Replaceable, Transliterator, UnicodeFilter}

class EmojiRemoverTransliterator (val ID: String, val filter: UnicodeFilter)
  extends Transliterator(ID, filter) with Serializable {

  def this() {
    this("Emoji-Remover", null)
  }

  override protected def handleTransliterate(
                                              text: Replaceable,
                                              pos: Transliterator.Position,
                                              incremental: Boolean)
  : Unit = {

    val replaced = new EmojiParserExtended().replaceAllEmojisWithSpace(text.toString)

    pos.start = pos.limit
    text.replace(0, replaced.length, replaced)
  }
}

object EmojiRemoverTransliterator {
  Transliterator.registerInstance(new EmojiRemoverTransliterator())
}
