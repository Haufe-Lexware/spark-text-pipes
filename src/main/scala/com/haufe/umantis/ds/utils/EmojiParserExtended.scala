package com.haufe.umantis.ds.utils

import com.vdurmont.emoji.EmojiParser

class EmojiParserExtended extends EmojiParser {
  def replaceAllEmojisWithSpace(str: String): String = {
    val emojiTransformer: EmojiParser.EmojiTransformer = new EmojiParser.EmojiTransformer() {
      override def transform(unicodeCandidate: EmojiParser.UnicodeCandidate) = " "
    }

    EmojiParser.parseFromUnicode(str, emojiTransformer)
  }
}
