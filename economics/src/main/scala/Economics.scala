import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)
    val iccM7PurchasingPrice = 35000.0
    val iccM7RentingPrice = 20.40
    val daysPerYear = 365
    val containerRamIccM7Factor = 24*64
    val ramContainerRentingPrice = 0.012
    val containerCPUIccM7Factor = 2*14
    val CPUContainerRentingPrice = 0.088
    val containerIccM7EqRentingPrice = containerRamIccM7Factor*ramContainerRentingPrice + containerCPUIccM7Factor*CPUContainerRentingPrice
    val iccM7VsContainerEq = iccM7RentingPrice / containerIccM7EqRentingPrice
    val containerRam4RPIEq = 4*8
    val containerCPU4RPIEq = 1.0
    val container4RPIEqRentingPrice = containerRam4RPIEq*ramContainerRentingPrice + containerCPU4RPIEq * CPUContainerRentingPrice
    val RPIMaxOperatingCost =  0.054
    val RPIMinOperatingPrice = 0.0108
    val containerVs4RPIMaxRentingPrice = 4 * RPIMaxOperatingCost / container4RPIEqRentingPrice
    val containerVs4RPIMinRentingPrice = 4 * RPIMinOperatingPrice / container4RPIEqRentingPrice

    val RPIPurchasingPrice = 94.83

    val RPICPU = 0.25
    val RPIRam = 8

    val RPIEqIccM7  = math.floor(iccM7PurchasingPrice / RPIPurchasingPrice)

    val userBytesStorage = 900

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
            "Q5.1.1" -> Map(
              "MinDaysOfRentingICC.M7" -> iccM7PurchasingPrice / iccM7RentingPrice, // Datatype of answer: Double
              "MinYearsOfRentingICC.M7" -> iccM7PurchasingPrice / (iccM7RentingPrice * daysPerYear)// Datatype of answer: Double
            ),
            "Q5.1.2" -> Map(

              "DailyCostICContainer_Eq_ICC.M7_RAM_Throughput" -> containerIccM7EqRentingPrice , // Datatype of answer: Double
              "RatioICC.M7_over_Container" -> iccM7VsContainerEq, // Datatype of answer: Double
              "ContainerCheaperThanICC.M7" ->  (abs(iccM7RentingPrice - containerIccM7EqRentingPrice) > 0.05*iccM7RentingPrice) // Datatype of answer: Boolean
            ),
            "Q5.1.3" -> Map(
              "DailyCostICContainer_Eq_4RPi4_Throughput" -> container4RPIEqRentingPrice, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MaxPower" ->containerVs4RPIMaxRentingPrice , // Datatype of answer: Double
              "Ratio4RPi_over_Container_MinPower" -> containerVs4RPIMinRentingPrice, // Datatype of answer: Double
              "ContainerCheaperThan4RPi" -> (math.abs(containerVs4RPIMaxRentingPrice - container4RPIEqRentingPrice) > 0.05 *containerVs4RPIMaxRentingPrice) // Datatype of answer: Booleanls

            ),
            "Q5.1.4" -> Map(
              "MinDaysRentingContainerToPay4RPis_MinPower" -> math.ceil(4*RPIPurchasingPrice / (container4RPIEqRentingPrice - 4*RPIMinOperatingPrice)), // Datatype of answer: Double
              "MinDaysRentingContainerToPay4RPis_MaxPower" ->math.ceil(4*RPIPurchasingPrice / (container4RPIEqRentingPrice - 4*RPIMaxOperatingCost)) // Datatype of answer: Double
            ),
            "Q5.1.5" -> Map(
              "NbRPisForSamePriceAsICC.M7" -> RPIEqIccM7 , // Datatype of answer: Double
              "RatioTotalThroughputRPis_over_ThroughputICC.M7" -> (RPIEqIccM7 * RPICPU) / containerCPUIccM7Factor , // Datatype of answer: Double
              "RatioTotalRAMRPis_over_RAMICC.M7" -> (RPIEqIccM7 * RPIRam) / containerRamIccM7Factor // Datatype of answer: Double
            ),
            "Q5.1.6" ->  Map(
              "NbUserPerGB" -> floor(1e9d*0.5 /userBytesStorage), // Datatype of answer: Double
              "NbUserPerRPi" -> floor( 8*1e9d*0.5 /userBytesStorage), // Datatype of answer: Double
              "NbUserPerICC.M7" -> floor( 24*64*1e9d*0.5 /userBytesStorage) // Datatype of answer: Double
            )
            // Answer the Question 5.1.7 exclusively on the report.
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
