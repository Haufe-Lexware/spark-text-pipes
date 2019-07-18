# Spark Text Pipes
Spark Text Pipes (STP) is a library for text processing and Feature creation 
for machine learning algorithms based
on [Spark](https://spark.apache.org/) and [Kafka](https://kafka.apache.org).
It's able to perform semantic match and search analysis over text fields, 
including retrieving geographical coordinates from pure textual data by querying 
[Dbpedia](https://wiki.dbpedia.org/).

## Architecture
The main component of the DSE is a long-lived Spark application that runs on a Spark cluster. 
It was designed to extensively employ Spark ML pipelines, so to apply sets of transformations 
over data. 

Multiple pipelines can operate over multiple fields of data in the same or different DataFrames, 
each transformer adding new columns (i.e. new fields) 
to the tables as data flows along the pipeline. 
Transformers can be combined freely and flexibly to adjust pipelines behaviours to the task at hand. 
Data can be injected in Spark through Kafka. Flexible interfaces and classes provide logic
to obtain always fresh in our DataFrames. 

### Transformers
For now, we have engineered transformers with three different purposes: 
- processing natural language text
- dealing with geographical data
- calculating scoring metrics or features. 

They all share the same interface with `spark.ml` (DataFrame API). These transformers 
are meant to be mixed together with other `spark.ml` transformers and estimators in the same pipeline, 
hence providing easy access to machine learning techniques.

For instance, text transformers can clean-up text, convert HTML to text, detect natural languages, 
filter words, calculate [word embeddings](https://en.wikipedia.org/wiki/Word_embedding), 
and perform other NLP transformations.
[Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP) is also supported in our pipelines.
Since transformers are are run in a distributed environment 
while also using non-serialisable code (e.g. language-detection libraries), 
we make extensive use of the 
[`@transient lazy val` pattern](http://fdahms.com/2015/10/14/scala-and-the-transient-lazy-val-pattern/). 

### Word Embeddings
Word embeddings refer to a set of techniques that aim to calculate vector 
representations of words. They were initially discussed in the research area of 
[distributional semantics](https://en.wikipedia.org/wiki/Distributional_semantics) 
and are used as a fundamental building block in modern-AI NLP.
They are calculated from large corpora of text, often Wikipedia, 
one semantic model for each language.
 
Distances between pairs of vectors can straightforwardly be employed to calculate 
and thus express similarities between pairs of documents. Interestingly, 
recent embeddings can solve most analogy relationships via linear algebra, e.g.
```
v(king) – v(man) + v(woman) ~= v(queen)
v(paris) – v(france) + v(germany) ~= v(berlin)
```
Conveniently, several research groups at major companies (e.g. Facebook, Google, etc.) 
distribute pre-trained embeddings under permissive licenses. 
We use Facebook's [Muse](https://github.com/facebookresearch/MUSE) vectors. 
They are state-of-the-art multilingual word embeddings 
([fastText embeddings](https://github.com/facebookresearch/fastText/blob/master/pretrained-vectors.md)
aligned in a common space). This allows us to calculate semantics similarities between vectors 
relative to words from different languages.

By aggregating vectors in paragraphs and documents, it is possible to define representations
for arbitrary pieces of texts, such as sentences, paragraphs, or entire documents. While a 
simple average of the words constituting a sentence is the most popular approach, 
we also provide classes that support more advanced techniques, such as the 
[Word Mover Distance](http://proceedings.mlr.press/v37/kusnerb15.pdf), 
a specialised version of the 
[Earth Mover Distance](https://en.wikipedia.org/wiki/Earth_mover%27s_distance). 
  
### Geographical Data
Thanks to Dbpedia doing the heavy lifting, we provide classes to find
 the geographical coordinates of places given their 
(possibly incomplete) name (and, optionally, their country), in multiple languages 
(including Asian languages). 

## Ranking Functions
### Textual fields similarity
Once semantic vectors are calculated for every text row of two `DataFrame`s `A` and `B`, 
for each row of `A`, it is possible to rank rows of `B` according to a similarity metric.

### Geographical similarity
Geographical similarities are calculated using the 
[Haversine formula](https://en.wikipedia.org/wiki/Haversine_formula).

## Kafka integration
STP supports reading from and writing to Kafka topics, including Avro-serialized topics
(only reading for now) through Spark's 
[Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). 
Streaming `DataFrame`s can be sinked to `Parquet` files. Logic to get fresh copies of these 
`DataFrame`s are provided (so that they can be used at the same time).

# Usage

## Deployment

STP has a few dependendencies. `Dockerfile`s are included under `docker` along with a
`docker-compose.yml` in `/`.
 
Given a user-defined `ROOT_DIR`, Docker services share two directories, `$ROOT_DIR/data` and
`$ROOT_DIR/apps`. While the former hosts data used and produced by STP and other services, the 
latter holds scripts and jar files. Most Docker containers will mount these two directories.

```bash
ROOT_DIR="../" # user-defined root directory
mkdir ${ROOT_DIR}/data
mkdir ${ROOT_DIR}/apps
mkdir ${ROOT_DIR}/apps/corenlp
mkdir ${ROOT_DIR}/apps/scripts

# If Stanford CoreNLP is used, download the models
sh scripts/get_stanford_corenlp_models.sh

# How to launch the Spark Shell for development (example)
cp scripts/spark-shell.sh ../apps/scripts

# By doing this the spark shell history is preserved after the container is gone
touch ../data/scala_history
```

## Additional Service Dependencies

STP has two major dependencies (git clone them in $ROOT_DIR):
- [Words-Embeddings-Dict](https://github.com/Haufe-Lexware/word-embeddings-dict) \
An akka-based microservice that acts as dictionary for multi-language word embeddings.
- [Text2Geolocation](https://github.com/Haufe-Lexware/Text2Geolocation) \
A REST microservice returning latitude and longitude from text.

Both services should be running before running STP.

## Building
### Install sbt
```
apt-get update
apt-get install apt-transport-https
echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
apt-get update
apt-get install sbt
```

### Building
```bash
 sbt 'set test in assembly := {}' assembly
 # If it fails, increase the RAM:
 # sbt -mem 4000 'set test in assembly := {}' assembly
 # you might need to pass a suitable java home (java 8 required) to sbt, e.g.
 # sbt -java-home /usr/lib/jvm/java-8-openjdk-amd64 'set test in assembly := {}' assembly
```

### Running the Spark Shell
```bash
# On the host machine
docker exec -it --env COLUMNS=`tput cols` --env LINES=`tput lines` master bash -c "stty cols $COLUMNS rows $LINES && /bin/bash"

# In the Docker container
/apps/scripts/spark-shell.sh
```
 
### Testing from command line
Inside a docker container with access to all necessary docker container dependencies, 
assuming your `$ROOT_DIR/apps/` is `/apps/`,

```bash
cd /apps/
git clone https://github.com/Haufe-Lexware/spark-text-pipes.git
cd spark-text-pipes
``` 

#### Running all tests
```bash
env TESTING="true" DATA_ROOT="/data/" APPS_ROOT="/apps/" sbt -ivy /apps/ivy -mem 20000 test
```

#### Running just a subset of the tests
```bash
env TESTING="true" DATA_ROOT="/data/" APPS_ROOT="/apps/" sbt -ivy /apps/ivy -mem 8000 "test:testOnly *TopicSource*"
```

### Running tests from idea
Setup environment variables

```bash
TESTING=true
DATA_ROOT=~/code/data
APPS_ROOT=~/code/apps
```

```DATA_ROOT``` corresponds to the directory where data is.

Please consider that both `EmbeddingsDict` and `Text2Geolocation` need to be reacheable for 
tests to work.

### Debugging the dependencies graph
```bash
sbt dependencyBrowseGraph
```

Hint: Edit the resulting html where `<svg width=1280 height=1024>` and change the windows size
if you wish so.  

## In your application

Define the `build.sbt` of your Spark/Scala application as follows:
```scala
name := "MySparkApp"
version := "0.1"
scalaVersion := "2.11.12"

lazy val root = (project in file(".")).dependsOn(
  RootProject(uri("https://github.com/Haufe-Lexware/spark-text-pipes.git"))
)

// Your apps path (deployment, scripts, etc. must be accessible by Spark)
val appsPath = sys.env.getOrElse("APPS_PATH", "../apps")

lazy val sparkDependencies = {
  val sparkVer = "2.4.3"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-avro" % sparkVer,
    "org.apache.spark" %% "spark-streaming" % sparkVer,
    "org.apache.spark" %% "spark-mllib" % sparkVer,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVer
  )
}

// spark
libraryDependencies ++= sparkDependencies.map(_ % "provided")
Test / test / libraryDependencies ++= sparkDependencies

// ALL YOUR APP DEPENDENCIES GO HERE
// libraryDependencies += ...
// libraryDependencies ++= Seq(...

// merge strategies 
assemblyMergeStrategy in assembly := {
  case PathList("reference.conf") => MergeStrategy.concat
  case x if x.contains("EncodingDetector") => MergeStrategy.deduplicate
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.last
}

// final Fat-Jar artifact
assemblyOutputPath in assembly := file(s"$appsPath/${name.value}.jar")

Project.inConfig(Test)(baseAssemblySettings)
assemblyJarName in (Test, assembly) := s"${name.value}-test-${version.value}.jar"

fork in ThisBuild in Test:= false
```

## Usage Examples

See `com.haufe.umantis.ds.examples.Search`.