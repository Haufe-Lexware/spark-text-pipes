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

package com.haufe.umantis.ds.nlp


import com.haufe.umantis.ds.location.{CoordinatesFetcher, DistanceCalculator, DistanceScorer}
import com.haufe.umantis.ds.nlp.stanford.corenlp.StanfordPipelines._
import com.haufe.umantis.ds.nlp.stanford.corenlp._
import com.haufe.umantis.ds.spark.{NormalizedBagOfWords, UnaryUDFTransformer}
import com.haufe.umantis.ds.utils.{ConfigGetter, URLDetector, URLExpander, URLValidator}
import com.haufe.umantis.ds.wmd.{EuclideanDistanceNormalized, WordMoverDistanceCalculator}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.RegexTokenizer


abstract class Colnames() extends Serializable {
  def score: String
}

class ColnamesText(val colName: String) extends Colnames {

  def text: String = colName

  def language: String = s"${colName}__language"

  def clean: String = s"${colName}__clean"

  def tokens: String = s"${colName}__tokens"

  def cleanWords: String = s"${colName}__words"

  def vector: String = s"${colName}__vector"

  def score: String = s"${colName}__score"

  def pos: String = s"${colName}__pos"

  def lemma: String = s"${colName}__lemma"

  def ner: String = s"${colName}__ner"

  def corenlpTokens: String = s"${colName}__corenlpTokens"

  def nBOW: String = s"${colName}__nBOW"
}
object ColnamesText {
  def apply(colName: String): ColnamesText = new ColnamesText(colName)
}

class ColnamesTextSimilarity(val baseText: ColnamesText,
                             val varyingText: ColnamesText)
  extends Colnames {

  def similarity: String =
    s"${baseText.vector}__similarity__${varyingText.vector}"

  def score: String =
    s"${baseText.vector}__score__${varyingText.vector}"
}
object ColnamesTextSimilarity {
  def apply(baseText: ColnamesText, varyingText: ColnamesText): ColnamesTextSimilarity =
    new ColnamesTextSimilarity(baseText, varyingText)
}

class ColnamesURL(val colName: String) extends Colnames {
  def text: String = colName

  def textAllLatin: String = s"${colName}__allLatin"

  def urls: String = s"${colName}__urls"

  def validURLs: String = s"${colName}__valid"

  def expandedURLs: String = s"${colName}__expanded"

  def score: String = s"${colName}__score"
}
object ColnamesURL {
  def apply(colName: String): ColnamesURL = new ColnamesURL(colName)
}

class ColnamesLocation(val locationCol: String, val countryCodeCol: Option[String] = None)
  extends Colnames {

  def location: String = locationCol

  def countryCode: String = countryCodeCol match {
    case Some(s) => s;
    case _ => ""
  }

  def coords: String = s"${locationCol}__coords"

  def score: String = s"{$locationCol}__score"
}
object ColnamesLocation {
  def apply(locationCol: String, countryCodeCol: Option[String] = None): ColnamesLocation =
    new ColnamesLocation(locationCol, countryCodeCol)
}


class ColnamesDistance(val firstLocation: ColnamesLocation,
                       val secondLocation: ColnamesLocation)
  extends Colnames {

  def distance: String =
    s"${firstLocation.location}__distance__${secondLocation.location}"

  def distanceFactor: String =
    s"${firstLocation.location}__distanceFactor__${secondLocation.location}"

  def score: String =
    s"${firstLocation.location}__score__${secondLocation.location}"
}
object ColnamesDistance {
  def apply(firstLocation: ColnamesLocation, secondLocation: ColnamesLocation): ColnamesDistance =
    new ColnamesDistance(firstLocation, secondLocation)
}

class ColnamesAggregated[T <: Colnames](val outputCol: String, val inputCols: Array[T])
  extends Colnames {

  def scores: Array[String] = inputCols.map(_.score)

  def score: String = outputCol

  def linearWeights: String = scores.mkString("", "__", "__linearWeights")

  def max: String = scores.mkString("", "__", "__max")
}
object ColnamesAggregated {
  def apply[T <: Colnames](outputCol: String, inputCols: Array[T]): ColnamesAggregated[T] =
    new ColnamesAggregated(outputCol, inputCols)
}

trait Stg {
  val TextCleaner: String = "TextCleaner"
  val TextCleanerWithoutAcronyms: String = "TextCleanerWithoutAcronyms"
  val LanguageDetector: String = "LanguageDetector"
  val Tokenizer: String = "Tokenizer"
  val StopWordsRemover: String = "StopWordsRemover"
  val EmbeddingsModel: String = "EmbeddingsModel"
  val OtherLanguageBooster: String = "OtherLanguageBooster"
  val SimilarityScorer: String = "SimilarityScorer"

  val WordMoverDistance: String = "WordMoverDistance"
  val NormalizedBagOfWords: String = "NormalizedBagOfWords"

  val CoordinatesFetcher: String = "CoordinatesFetcher"
  val DistanceCalculator: String = "DistanceCalculator"
  val DistanceScorer: String = "DistanceScorer"

  val Max: String = "ColumnAggregatorMax"
  val Min: String = "ColumnAggregatorMin"
  val Sum: String = "ColumnAggregatorSum"
  val Mean: String = "ColumnAggregatorMean"
  val LinearWeigher: String = "LinearWeigher"

  val URLAllLatin: String = "URLAllLatin"
  val URLDetector: String = "URLDetector"
  val URLValidator: String = "URLValidator"
  val URLExpander: String = "URLExpander"
}

object Stg extends Stg

object StandardPipeline {
  val TextDataPreprocessing: Seq[String] =
    Seq(
      Stg.LanguageDetector,
      Stg.TextCleaner,
      Stg.Tokenizer,
      Stg.StopWordsRemover,
      Stg.EmbeddingsModel
    )

  val TextDataScoringCentroid: Seq[String] =
    Seq(
      Stg.SimilarityScorer,
      Stg.OtherLanguageBooster
    )

  val DistanceScoring: Seq[String] = Seq(
    Stg.DistanceCalculator,
    Stg.DistanceScorer
  )

  val URLProcessing: Seq[String] = Seq(
    Stg.URLAllLatin,
    Stg.URLDetector,
//    Stg.URLValidator,
    Stg.URLExpander
  )
}

case class DsPipelineInput[T <: Colnames](cols: Seq[T], stages: Seq[String])
object DsPipelineInput {
  def apply[T <: Colnames](cols: T, stages: String): DsPipelineInput[T] =
    DsPipelineInput(Seq(cols), Seq(stages))

  def apply[T <: Colnames](cols: Seq[T], stages: String): DsPipelineInput[T] =
    DsPipelineInput(cols, Seq(stages))

  def apply[T <: Colnames](cols: T, stages: Seq[String]): DsPipelineInput[T] =
    DsPipelineInput(Seq(cols), stages)
}

class DsPipeline(input: Seq[DsPipelineInput[Colnames]]) {

  def companion[T <: AnyVal with DsPipelineCommon]: T = DsPipeline.asInstanceOf[T]

  def stages: Array[Transformer] = {
    input.flatMap(i =>
      i.stages.flatMap(stageName =>
        i.cols.map(col =>
          companion.stagesOptions(stageName).asInstanceOf[Colnames => Transformer](col)
        )
      )
    ).toArray
  }

  def pipeline: PipelineExtended = new PipelineExtended().setStages(stages)

  def pipelineAddedFields: Array[String] =
    stages.map(s => {
      val param = s.getParam("outputCol")
      s.getOrDefault(param).toString
    })
}

trait DsPipelineCommon extends ConfigGetter {

  lazy val stopWordsMap: Map[String, Set[String]] =
    MultiLanguageStopWordsRemover
      .loadDefaultStopWordsMap(LanguagesConfiguration.supportedLanguages)

  def getNormalizedBagOfWordsTransformer(c: ColnamesText):
    UnaryUDFTransformer[Seq[String], Seq[(String, Int)]] =
      NormalizedBagOfWords()
        .setInputCol(c.tokens)
        .setOutputCol(c.nBOW)

  def getTextCleaner(c: ColnamesText): TextCleaner =
    new TextCleaner()
      .setInputCol(c.text)
      .setOutputCol(c.clean)

  def getTextCleanerWithoutAcronyms(c: ColnamesText): TextCleaner =
    getTextCleaner(c)
      .setExpandAcronyms(false)

  def getLanguageDetector(c: ColnamesText): LanguageDetector =
    new LanguageDetector()
      .setSupportedLanguages(LanguagesConfiguration.supportedLanguages)
      .setDefaultLanguage("en")
      .setInputCol(c.text)
      .setOutputCol(c.language)

  def getTokenizer(c: ColnamesText): RegexTokenizer =
    new RegexTokenizerExtended()
      .setInputCol(c.clean)
      .setOutputCol(c.tokens)
      .setMinTokenLength(2)
      .setToLowercase(false)
      .setPattern("""[^\p{Ll}\p{Lu}0-9]""")

  def getStopWordsRemover(c: ColnamesText): MultiLanguageStopWordsRemover =
    new MultiLanguageStopWordsRemover()
      .setStopWordsMap(stopWordsMap)
      .setDefaultLanguage("en")
      .setLanguageCol(c.language)
      .setInputCol(c.tokens)
      .setOutputCol(c.cleanWords)

  def getWordMoverDistance(c: ColnamesTextSimilarity): WordMoverDistanceCalculatorTransformer =
    new WordMoverDistanceCalculatorTransformer(WordMoverDistanceCalculator())
      .setCalculationType(EuclideanDistanceNormalized)
      .setInputCol(c.varyingText.nBOW)
      .setLanguageCol(c.varyingText.language)
      .setBaseLanguageCol(c.baseText.language)
      .setBaseDocumentCol(c.baseText.nBOW)
      .setOutputCol(c.similarity)

  def getStanfordCoreNLP(c: ColnamesText): StanfordCoreNLPTransformer =
    StanfordCoreNLPTransformer(StanfordPipelines(
      ("en", Tokenize(c.corenlpTokens), Pos(c.pos), Lemma(c.lemma), Ner(c.ner)),
      ("de", Tokenize(c.corenlpTokens), Pos(c.pos), Ner(c.ner))))
      .setDefaultLanguage("en")
      .setLanguageCol(c.language)
      .setInputCol(c.clean)

  def getEmbeddingsModel(c: ColnamesText): EmbeddingsModel =
    EmbeddingsModel()
      .setSupportedLanguages(LanguagesConfiguration.supportedLanguages)
      .setDefaultLanguage("en")
      .setLanguageCol(c.language)
      .setInputCol(c.cleanWords)
      .setOutputCol(c.vector)

  def getOtherLanguageBooster(c: ColnamesTextSimilarity): OtherLanguagesBooster =
    new OtherLanguagesBooster()
      .setBoostsMap(LanguagesConfiguration.defaultOtherLanguageBoosts)
      .setInputCol(c.similarity)
      .setOutputCol(c.score)
      .setLanguageCol(c.varyingText.language)
      .setBaseLanguageCol(c.baseText.language)

  def getSimilarityScorerDenseVector(c: ColnamesTextSimilarity): SimilarityScorerDenseVector =
    new SimilarityScorerDenseVector()
      .setInputCol(c.varyingText.vector)
      .setOutputCol(c.similarity)
      .setBaseVectorCol(c.baseText.vector)

  def getCoordinatesFetcher(c: ColnamesLocation): CoordinatesFetcher =
    new CoordinatesFetcher()
      .setLocationCol(c.location)
      .setCountryCol(c.countryCode)
      .setOutputCol(c.coords)

  def getDistanceCalculator(c: ColnamesDistance): DistanceCalculator =
    new DistanceCalculator()
      .setInputCol(c.firstLocation.coords)
      .setBaseLocationCol(c.secondLocation.coords)
      .setOutputCol(c.distance)

  def getDistanceScorer(c: ColnamesDistance): DistanceScorer =
    new DistanceScorer()
      .setInputCol(c.distance)
      .setOutputCol(c.score)
      .setDistanceFactorCol(c.distanceFactor)

  def getLinearWeigher[T <: Colnames](c: ColnamesAggregated[T]): LinearWeigher =
    new LinearWeigher()
      .setInputCols(c.scores)
      .setOutputCol(c.score)
      .setLinearWeightsCol(c.linearWeights)

  def getColumnAggregator[T <: Colnames](
                                          c: ColnamesAggregated[T],
                                          aggregationFunction: Seq[Float] => Float
                                        )
  : ColumnsAggregator =
    new ColumnsAggregator()
      .setInputCols(c.scores)
      .setOutputCol(c.score)
      .setAggregationFunction(aggregationFunction)

  def getMax[T <: Colnames](c: ColnamesAggregated[T]): ColumnsAggregator =
    getColumnAggregator(c, ColumnsAggregator.max)

  def getMin[T <: Colnames](c: ColnamesAggregated[T]): ColumnsAggregator =
    getColumnAggregator(c, ColumnsAggregator.min)

  def getSum[T <: Colnames](c: ColnamesAggregated[T]): ColumnsAggregator =
    getColumnAggregator(c, ColumnsAggregator.sum)

  def getMean[T <: Colnames](c: ColnamesAggregated[T]): ColumnsAggregator =
    getColumnAggregator(c, ColumnsAggregator.mean)

  def getTextToLatin(c: ColnamesURL): ICUTransformer =
    new ICUTransformer()
      .setTransliteratorID("Emoji-Remover; Any-Latin; Latin-ASCII")
      .setInputCol(c.text)
      .setOutputCol(c.textAllLatin)

  def getURLDetector(c: ColnamesURL): URLDetector =
    new URLDetector()
      .setInputCol(c.textAllLatin)
      .setOutputCol(c.urls)

  def getURLValidator(c: ColnamesURL): URLValidator =
    new URLValidator()
      .setInputCol(c.urls)
      .setOutputCol(c.validURLs)

  def getURLExpander(c: ColnamesURL): URLExpander =
    new URLExpander()
      .setInputCol(c.urls)
      .setOutputCol(c.expandedURLs)

  def stagesOptions: Map[String, _ => Transformer] =
    Map[String, _ => Transformer](
      Stg.TextCleaner -> getTextCleaner _,
      Stg.TextCleanerWithoutAcronyms -> getTextCleanerWithoutAcronyms _,
      Stg.LanguageDetector -> getLanguageDetector _,
      Stg.Tokenizer -> getTokenizer _,
      Stg.StopWordsRemover -> getStopWordsRemover _,
      Stg.EmbeddingsModel -> getEmbeddingsModel _,
      Stg.OtherLanguageBooster -> getOtherLanguageBooster _,
      Stg.SimilarityScorer -> getSimilarityScorerDenseVector _,

      Stg.WordMoverDistance -> getWordMoverDistance _,
      Stg.NormalizedBagOfWords -> getNormalizedBagOfWordsTransformer _,

      Stg.CoordinatesFetcher -> getCoordinatesFetcher _,
      Stg.DistanceCalculator -> getDistanceCalculator _,
      Stg.DistanceScorer -> getDistanceScorer _,

      Stg.URLAllLatin -> getTextToLatin _,
      Stg.URLDetector -> getURLDetector _,
      Stg.URLValidator -> getURLValidator _,
      Stg.URLExpander -> getURLExpander _,

      Stg.Max -> getMax _,
      Stg.Min -> getMin _,
      Stg.Sum -> getSum _,
      Stg.Mean -> getMean _,
      Stg.LinearWeigher -> getLinearWeigher _
    )
}

object DsPipeline extends DsPipelineCommon {
  def apply(input: DsPipelineInput[Colnames]): DsPipeline = new DsPipeline(Seq(input))
}