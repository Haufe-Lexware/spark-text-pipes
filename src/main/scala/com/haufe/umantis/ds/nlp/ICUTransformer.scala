package com.haufe.umantis.ds.nlp

import com.ibm.icu.text.Transliterator

import scala.collection.mutable

object ICUTransliterators extends Serializable {

  private var defaultTransformation = "Lower; Any-Latin; Latin-ASCII"

  @transient lazy val transliterators: mutable.Map[String, Transliterator] = mutable.Map()

  def getTransformer(transformation: String): Transliterator = {
    transliterators.getOrElseUpdate(transformation, Transliterator.getInstance(transformation))
  }

  implicit class StringTransliterator(in: String) {
    def transliterateWith(transformation: String): String = {
      getTransformer(transformation).transliterate(in)
    }

    def transliterate: String = {
      transliterateWith(defaultTransformation)
    }
  }
}


class ICUTransformer {

}


