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

package com.haufe.umantis.ds.spark

import com.haufe.umantis.ds.tests.SparkSpec
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers._
import org.scalatest.tagobjects.Slow



class EnginePerformanceSpec extends SparkSpec {
  val sc: SparkContext = currentSparkSession.sparkContext
  val spark: SparkSession = currentSparkSession


  // change ignore with "Spark" to run it
  ignore should " make computation" taggedAs Slow in {
    import org.apache.spark.ml.linalg.{DenseVector, Vectors}
    import org.apache.spark.mllib.linalg.DenseMatrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    import org.apache.spark.sql.functions._
    import spark.implicits._

    import scala.util.Random

    val dotVector = udf { (x: DenseVector, y: DenseVector) => {
      var i = 0
      var dotProduct = 0.0
      val size = x.size
      val v1 = x.values
      val v2 = y.values
      while (i < size) {
        dotProduct += v1(i) * v2(i)
        i += 1
      }
      dotProduct
    }
    }
    val dotSeq = udf { (x: Seq[Double], y: Seq[Double]) => {
      var i = 0
      var dotProduct = 0.0
      val size = x.size
      while (i < size) {
        dotProduct += x(i) * y(i)
        i += 1
      }
      dotProduct
    }
    }

    val dotProduct = (x: Seq[Double], y: Seq[Double]) => {
      var i = 0
      var dotProduct = 0.0
      val size = x.size
      while (i < size) {
        dotProduct += x(i) * y(i)

        i += 1
      }
      dotProduct
    }

    def time(name: String, block: => Unit): Float = {
      val t0 = System.nanoTime()
      block // call-by-name
      val t1 = System.nanoTime()
      //println(s"$name: " + (t1 - t0) / 1000000000f + "s")
      (t1 - t0) / 1000000000f
    }

    val densevector = udf { p: Seq[Double] => Vectors.dense(p.toArray) }
    val floatToDenseVector = udf { p: Seq[Float] => Vectors.dense(p.map(_.toDouble).toArray) }

    val genVec = udf { (l: Int, c: Int) => {
      val r = new Random(l * c)
      (1 to 300).map(p => r.nextDouble()).toArray
    }
    }

    val base = (1 to 300).map(p => new Random().nextDouble()).toArray
    val dfBig = {
      Seq(1).toDF("s")
        .withColumn("line", explode(lit((1 to 1000).toArray)))
        .withColumn("column", explode(lit((1 to 200).toArray)))
        .withColumn("v1",
          genVec(col("line").*(lit(-1)), col("column")))
        .withColumn("v2", lit(base))
        .withColumn("v1d", densevector(col("v1")))
        .withColumn("v2d", densevector(col("v2")))
        .repartition(1)
        .persist()
    }
    println(s"dbBig.count: ${dfBig.count}")
    dfBig.show(10)
    val rdd = {
      sc.parallelize(Seq(1))
        .flatMap(f => (1 to 1000).map((f, _)))
        .flatMap(f => (1 to 200).map((f._1, f._2, _)))
        .map(f => {
          val r = new Random(-f._2 * f._3)
          (f._1, f._2, f._3, (1 to 300).map(p => r.nextDouble()).toArray)
        })
        .repartition(1)
        .persist()
    }
    println(s"rdd.count: ${rdd.count}")

    val rowmatrix = new RowMatrix(rdd.map(f => org.apache.spark.mllib.linalg.Vectors.dense(f._4))
      .persist())
    val dm = new DenseMatrix(1, 300, base)

    val rddMatrixTime = (1 to 20).map { p =>
      time("rddMatrixTime",
        rowmatrix.multiply(dm.transpose).rows
          .zipWithIndex.sortBy(_._1(0), ascending = false).take(10))
    }.sum / 20
    println(s"rddMatrixTime: $rddMatrixTime")

    val rddTime = (1 to 20).map { p =>
      time("rddTime",
        rdd.map(p => (p._2, p._3, dotProduct(p._4, base)))
          .sortBy(_._3, ascending = false).take(10))
    }.sum / 20
    println(s"rddTime: $rddTime")

    val arrayTime = (1 to 20).map { p =>
      time("array", dfBig
        .withColumn("dot", dotSeq(col("v1"), col("v2")))
        .select("line", "column", "dot")
        .sort(desc("dot")).limit(10).collect())
    }.sum / 20
    println(s"arrayTime: $arrayTime")

    val vectorTime = (1 to 20).map { p =>
      time("vectorTime",
        dfBig.withColumn("dot", dotVector(col("v1d"), col("v2d")))
          .select("line", "column", "dot")
          .sort(desc("dot")).limit(10).collect())
    }.sum / 20
    println(s"vectorTime: $vectorTime")
  }

}
