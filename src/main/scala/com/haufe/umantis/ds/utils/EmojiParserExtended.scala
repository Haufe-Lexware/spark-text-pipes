package com.haufe.umantis.ds.utils

import com.vdurmont.emoji.EmojiParser
import com.vdurmont.emoji.EmojiParser.UnicodeCandidate

import scala.collection.JavaConverters._

class EmojiParserExtended extends EmojiParser with Serializable {
  def getAllUnicodeCandidates(input: String): List[UnicodeCandidate] =
    EmojiParser
      .getUnicodeCandidates(input)
      .asScala
      .toList
}
