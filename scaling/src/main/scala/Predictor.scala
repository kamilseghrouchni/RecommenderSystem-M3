import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._

import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String]()
  verify()
}

object Predictor {
  def main(args: Array[String]) {
    var conf = new Conf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())
    for (line <- trainFile.getLines) {
      val cols = line.split(conf.separator()).map(_.trim)
      trainBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0,9)) + "s")

    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()
    println("Compute kNN on train data...")

    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())
    for (line <- testFile.getLines) {
      val cols = line.split(conf.separator()).map(_.trim)
      testBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close

    println("Compute predictions on test data...")


    def activeRatings(data: CSCMatrix[Double]) = {
      data.mapActiveValues(_=>1.0)
    }

    /*
  Compute user average
  @ param CSMatrix[Double](users,items)
  @ return CSMatrix[Double](user,1)
   */
    def ComputeUserAverage(data: CSCMatrix[Double] ) = {

      val userReducer = DenseVector.ones[Double](data.cols)

      (data * userReducer) /:/ (data.mapActiveValues(_=>1.0) * userReducer)
    }




    /*
    scale function
    @param : user specific weighted sum deviation : Double
    @param : user average : Double
    @ return : rescaled rating

*/
    def scale(x: Double, user_avg: Double): Double = {
      x match {
        case _ if x > user_avg => 5.0 - user_avg
        case _ if x < user_avg => user_avg - 1.0
        case _ if x == user_avg => 1.0
      }
    }

    /* Compute normalized deviation for user
  @param user ratings : CSCMatrix[Double]
  @param user average : DenseVector[Double]
  @return normalized deviation : CSCMatrix[Double] (user, movies)

 */

    def normalizedDeviation(ratings: CSCMatrix[Double], userAverage: DenseVector[Double]) = {
      // declare empty sparse matrix to fill with scaled ratings

      val scaledRatings = new CSCMatrix.Builder[Double](rows = ratings.rows, cols = ratings.cols)
      // movies users
      for (((user,movie), v) <- ratings.activeIterator) {
        // compute normalized deviation on scaled ratings

        scaledRatings.add(user, movie, (v - userAverage(user)) / scale(v, userAverage(user)))
      }
      scaledRatings.result()
    }

    /* Compute preprocessed ratings
     @param : normalized ratings CSCMatrix[Double]
     @return : preprocessed ratings : CSCMatrix[Double] (users, movies)
      */
    def preprocessedRatings(normalizedRatings: CSCMatrix[Double]) = {
      // declare empty sparse matrix to fill with preprocessed ratings
      val preprocessed = new CSCMatrix.Builder[Double](rows = normalizedRatings.rows, cols = normalizedRatings.cols)
      // compute denominator
      val normalizer = sqrt(pow(normalizedRatings, 2) * DenseVector.ones[Double](normalizedRatings.cols))
      // iterate over non zero normalized ratings
      for (((user,movie), v) <- normalizedRatings.activeIterator) {
        // if denominator not null
        val res = if (normalizer(user) != 0.0)  v/normalizer(user) else 0.0
        preprocessed.add(user, movie, res)
      }
      preprocessed.result()
    }


    /* Compute the similarities
     @param : preprocessed ratings
     @param : user : int
     @return similarities for the user : DenseVector[Double]
      */
    def similarities(preprocessedRatings: CSCMatrix[Double],user : Int) = {
      // convert to dense vector for faster computations
      val simis =preprocessedRatings *preprocessedRatings(user,0 to preprocessedRatings.cols-1 ).t.toDenseVector
      // put self similarities to sero
      simis(user)=0.0
      simis
      // similarity checked and correct
      // meaning preprocessing is correct
      // meaning user average is correct
    }


/* Compute top k nearest neighbors
@param user : Int
@param top K : Int
@param preprocessed ratings : CSCMatrix[Double] (user,movies)
@return : user Int, top k similarities  IndexedSeq[(Int,Double)]

 */

    def topk(user : Int,topK : Int, preprocessedRatings : CSCMatrix[Double] ) ={
      // compute similarities for that user
       val similarity = similarities(preprocessedRatings,user)
        (user,argtopk(similarity,topK).map(v=> (v,similarity(v))))}

    /* Parallelize knn
    @param preprocessed ratings CSCMatrix[Double]
    @param top k Int
    @param user :Int
    @param Spark Context
     */
    def parallelKnn (preprocessedRatings: CSCMatrix[Double], topK: Int, sc : SparkContext) ={
      // declare sparse matrix for nearest neighbors
      val nearestNeighbors = new CSCMatrix.Builder[Double](rows =preprocessedRatings.rows, cols = preprocessedRatings.rows)
      // broadcast ratings
      val br = sc.broadcast(preprocessedRatings)
      // send nearest neighbors computations to all nodes
      val topks = sc.parallelize(0 to preprocessedRatings.rows-1).map(v => topk(v,topK,br.value)).collect()
      topks.map { case (userU, simis) => {
        simis.map { case (userV, simi) => {
          nearestNeighbors.add( userV,userU, simi)
        }
        }

      }
      }
        nearestNeighbors.result()}



    /*Compute user weighted average deviation
    @param neighbors CSCMatrix[Double]
    @param normalized ratings CSCMatrix[Double]
    @param user Int, item Int
    @return Weighted sum deviation : Double
     */
    def userWeightedSumDeviation(neighbors: CSCMatrix[Double], normalizedRatings: CSCMatrix[Double],user : Int, item : Int  ) = {
      // select ratings for given item

      val selectedRatings = normalizedRatings( 0 to neighbors.cols - 1,item).toDenseVector
      // select neighbors for given user

      val selectedNeighbors = neighbors(  0 to neighbors.cols - 1,user).toDenseVector
      var num = 0.0
      var denom = 0.0
      // compute user weighted sum deviation

      for ((userV,rating)<-selectedRatings.activeIterator){
        num = num + rating * selectedNeighbors(userV)
        denom = if (rating !=0.0) denom+ math.abs(selectedNeighbors(userV)) else denom
      }
      val res = if (denom == 0.0) 0.0 else num/denom

      res

    }
    /*
    Compute ratings prediction
    @param : train CSCMatrix[Double]
    @param : test CSCMatrix[Double]
    @param : top K : Int
    @return predicted ratings : CSCMatrix[Double]

   */
    var timeKnn  = 0.0
    def parallelPredict(train: CSCMatrix[Double], test: CSCMatrix[Double], topK: Int) = {
      val predictions = new CSCMatrix.Builder[Double](rows = train.rows, cols = train.cols)

      val startKnn = System.nanoTime()
      // compute user average
      val userAverage = ComputeUserAverage(train)
      // broadcast user average
      val brUserAverage = sc.broadcast(userAverage)
      // compute normalized deviation
      val normalizedDev = normalizedDeviation(train, userAverage)
      // compute preprocessed ratings
      val prePro = preprocessedRatings(normalizedDev)
      // broadcast ratings
      val brNormalizedDev = sc.broadcast(normalizedDev)
      // compute parallel knn
      val neighbors = parallelKnn(prePro, topK, sc)
      // broadcast neighbors
      val brNeighbors = sc.broadcast(neighbors)
      val endKnn = System.nanoTime()
      timeKnn = (endKnn - startKnn) / 1e3

      /*
      Compute prediction for single user and item
       */
      def predictProcedure(userU: Int, itemI: Int) = {
        val userAvg = brUserAverage.value(userU)
        val userDev = userWeightedSumDeviation(brNeighbors.value, brNormalizedDev.value,userU,itemI)

        (userU, itemI, userAvg + userDev * scale(userAvg + userDev, userAvg))
      }

      val parallelPredictions = sc.parallelize( test.activeKeysIterator.toSeq).map{
        case (user,item)=>{
        predictProcedure(user,item)}}.collect().map{case (userU,itemI,pred)=> ((userU,itemI),pred)}.toMap

      for ((userU,itemI) <- test.activeKeysIterator){
        predictions.add(userU,itemI,parallelPredictions(userU,itemI))
      }


      predictions.result()}


    /*


 */
    def meanAbsoluteError(test: CSCMatrix[Double], prediction: CSCMatrix[Double]) = {

      sum(abs(test - prediction)) / test.activeSize
    }
    val startPrediction = System.nanoTime()
    val predictions = parallelPredict(train,test,200)
    val endPrediction = System.nanoTime()



    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
      }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {
        var json = "";
        {
          // Limiting the scope of implicit formats with {}
          implicit val formats = org.json4s.DefaultFormats

          val answers: Map[String, Any] = Map(
            "Q4.1.1" -> Map(
              "MaeForK=200" -> meanAbsoluteError(test,predictions) // Datatype of answer: Double
            ),
            // Both Q4.1.2 and Q4.1.3 should provide measurement only for a single run
            "Q4.1.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> timeKnn  // Datatype of answer: Double
            ),
            "Q4.1.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> (endPrediction - startPrediction) / 1e3  // Datatype of answer: Double
            )
            // Answer the other questions of 4.1.2 and 4.1.3 in your report
          )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  }
}
