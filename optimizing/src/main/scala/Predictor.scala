import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import time._
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

/*
Time object  :

@param : List of time measurements : List[Int]
return : sugar functions of average and standard deviation computation
on time measurements
*/
object time {

  implicit class time_stats (times : List[Double]) {
    // return average on list of time measurements
    def avg  :Double  = times.sum/times.length

    // return standard deviation  on list of time measurements
    def stdev: Double  = {
      val avgTime = times.avg
      scala.math.sqrt(times.map(r=> scala.math.pow(r-avgTime,2)).sum/ times.length)
    }
  }
}
object Predictor {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

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


    /*

    Time prediction and similarities
    @param : function call (block)
    @output : microsecond measurements over 5 runs

     */
    object Timer {

      // return time measurements for prediction and similarity
      def timer(t: => Any): (List[Double], List[Double]) = {
        val times = (1 to 5).map { r =>
          val start = System.nanoTime()
          val _ = t
          val end = System.nanoTime()
          ((end - start) / 1e3, simiTime)
        }.toList
        (times.map(_._1), times.map(_._2))
      }

      // update similarity time measurement when called
      def updateSimiTime(value: Double): Unit = {
        simiTime = simiTime + (value / 1e3)
      }


      /*
       variable to update similarity time measurement
       at each iteration
       */
      var simiTime = 0.0
    }

    /*
    Map the active ratings to 1
    @param CSCMatrix[Double]
    @output : CSCMatrix[Double]
     */
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
    @return normalized deviation : CSCMatrix[Double]

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

    }

    /*Compute k-nearest neighbors
     @param : preprocessed ratings : CSCMatrix[Double] (user, movies)
     @param : top k users ::Int
     @return : k-nearest neighbors  : CSCMatrix[Double] (users, users)

     */
    def knn(preprocessedRatings: CSCMatrix[Double], topK: Int) = {
      // declare empty matrix to fill with top k similarities for all users
      val nearestNeighbors = new CSCMatrix.Builder[Double](rows =preprocessedRatings.rows, cols = preprocessedRatings.rows)
      // iterate over users
      (0 to preprocessedRatings.rows - 1).foreach( k => {
        // compute similarities
        val simis = similarities(preprocessedRatings,k)
        // keep top k similarities
        for (j <- argtopk(simis, topK)) {
          // fill nearest neighbors
          nearestNeighbors.add( j,k, simis(j))
        }
      })
      nearestNeighbors.result()
    }
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
      for ((userV, rating) <- selectedRatings.activeIterator) {
        num = num + rating * selectedNeighbors(userV)
        denom = if (rating != 0.0) denom + math.abs(selectedNeighbors(userV)) else denom
      }
      val res = if (denom == 0.0) 0.0 else num / denom

      res
    }

    /*
      Compute ratings prediction
      @param : train CSCMatrix[Double]
      @param : test CSCMatrix[Double]
      @param : top K : Int
      @return predicted ratings : CSCMatrix[Double]

     */
    def predictor(train: CSCMatrix[Double], test: CSCMatrix[Double], topK: Int) = {

       // compute user average
      val userAverage = ComputeUserAverage(train)

      // compute normalized deviation
      val normalizedDev = normalizedDeviation(train, userAverage)
      // compute preprocessed ratings
      val prePro = preprocessedRatings(normalizedDev)
      // time similarities
      Timer.simiTime = 0.0
      val start_2 = System.nanoTime()
      // compute neighbors
      val neighbors = knn(prePro, topK)
      val end_2 = System.nanoTime()
      Timer.updateSimiTime(end_2-start_2)


      val start_6 = System.nanoTime()


      // declare sparse matrix for prediction
      val pred = new CSCMatrix.Builder[Double](rows = train.rows, cols = train.cols)
      // compute prediction for all items and movies in test set
      for (((user,movie), v) <- test.activeIterator) {
        val userDeviation = userWeightedSumDeviation(neighbors, normalizedDev,user,movie)
         val valuePred = userAverage(user) + userDeviation * scale(userAverage(user) +userDeviation, userAverage(user))
        pred.add(user, movie, valuePred)

      }
      println("userDeviation time " + (System.nanoTime -start_6 )/ pow(10.0, 9) + "s")


      pred.result()
    }

    /* Compute mean absolute error


 */
    def meanAbsoluteError(test: CSCMatrix[Double], prediction: CSCMatrix[Double]) = {

      sum(abs(test - prediction)) / sum(test.mapActiveValues(_=> 1.0))
    }


    println("Compute kNN on train data...")

    println("Compute predictions on test data...")
    val read_start_2 = System.nanoTime
    val predicted200 = predictor(train, test, 200)
    val read_duration_2 = System.nanoTime - read_start_2
    println(" test "+meanAbsoluteError(test,predicted200))

     val predicted100 = predictor(train, test, 100)

    val timePrediction =Timer.timer(predictor(train, test,200))

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
            "Q3.3.1" -> Map(
              "MaeForK=100" -> meanAbsoluteError(test, predicted100) , // Datatype of answer: Double
              "MaeForK=200" ->meanAbsoluteError(test, predicted200)  // Datatype of answer: Double
            ),
            "Q3.3.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> Map(
                "min" -> timePrediction._2.min,  // Datatype of answer: Double
                "max" -> timePrediction._2.max, // Datatype of answer: Double
                "average" -> timePrediction._2.avg, // Datatype of answer: Double
                "stddev" -> timePrediction._2.stdev // Datatype of answer: Double
              )
            ),
            "Q3.3.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> Map(
                "min" -> timePrediction._1.min,  // Datatype of answer: Double
                "max" -> timePrediction._1.max, // Datatype of answer: Double
                "average" ->timePrediction._1.avg, // Datatype of answer: Double
                "stddev" -> timePrediction._1.stdev  // Datatype of answer: Double
              )
            )
            // Answer the Question 3.3.4 exclusively on the report.
          )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  }
}









