import scala.io.Source
import scala.util.{Failure, Success, Try}

case class Transaction(
                        txid: String,
                        refid: String,
                        time: String,
                        `type`: String,
                        subtype: String,
                        aclass: String,
                        asset: String,
                        amount: String,
                        fee: String,
                        balance: String
                      )

case class Reward(symbol: String, amount: Double)

case class RewardResult(symbol: String,
                  baseAmount: Double,
                  gbpAmount: Double,
                  conversionRate: Double,
                  desc: String)

object StakingCalculator {
  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      args.foreach(println)
      val rewardsBySymbol = aggregate(args(0))
      rewardsBySymbol.foreach(println)
      val gbpRewards = rewardsBySymbol.map(_.gbpAmount).sum
      println(s"total rewards in GBP ${gbpRewards}")
    } else {
      println("Need to provide the file with the exported taxes")
    }
  }

  def aggregate(filePath: String) = {
    val bufferedSource = Source.fromFile(filePath)
    val csvLines = bufferedSource.getLines().toList
    bufferedSource.close()
    csvLines
      .map(_.split(",").map(_.trim))
      .map(r => Transaction(
        r(0),
        r(1),
        r(2),
        r(3),
        r(4),
        r(5),
        r(6).replace("\"",""),
        r(7).replace("\"",""),
        r(8).replace("\"",""),
        r(9).replace("\"","")))
      .filter(_.`type`.contains( "staking"))
      .map(tx => Reward(tx.asset, tx.amount.toDouble))
      .groupBy(_.symbol)
      .map {
        case (symbol, stakingTxs) =>
        Reward(symbol, stakingTxs.foldRight(0.0)((t, acc) => acc + t.amount))
      }
      .map( reward => {
        fetchPriceToGbp(reward.symbol) match {
          case Success(toGBP) =>
            RewardResult(reward.symbol, reward.amount, reward.amount*toGBP, toGBP, "Success")
          case Failure(err) =>
            RewardResult(reward.symbol, reward.amount, 0, 0, err.toString)
        }
      })
  }

  def fetchPriceToGbp(symbol: String): Try[Double] = {
      val refinedSymbol = symbol match {
        case "ETH2" => "ETH"
        case "XETH" => "ETH"
        case x if x.contains(".S") => x.replace(".S", "")
      }
    val url = s"https://api.binance.com/api/v3/ticker/price?symbol=${refinedSymbol}GBP"

    Try(Source.fromURL(url).mkString)
      .flatMap(parsePrice)
  }

  // can't use parsing library in script without having to include that in classpath
  // so just getting the price without JSON
  def parsePrice(json: String): Try[Double] = {
    val priceKey = "\"price\":"
    val startIndex = json.indexOf(priceKey)

    if (startIndex != -1) {
      val valueStartIndex = startIndex + priceKey.length
      val valueEndIndex = json.indexOf(',', valueStartIndex) match {
        case -1 => json.indexOf('}', valueStartIndex)
        case index => index
      }
      val price = json.substring(valueStartIndex, valueEndIndex).trim().replace("\"","").toDouble
      Success(price)
    } else {
      Failure(new RuntimeException("price not found"))
    }
  }

}



