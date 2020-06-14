import CustomOptimizationExample.MultiplyOptimizationRule
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()



    //////////////////////////////////////// Test Added Extra Logical Optimized Rule /////////////////////////////////////////////////

    val df = sparkSession.read.option("header","true").csv("src/main/resources/sales.csv")
    val multipliedDF = df.selectExpr("amountPaid * 1")
    println(multipliedDF.queryExecution.optimizedPlan.numberedTreeString)

    //add our custom optimization
    sparkSession.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("amountPaid * 1")
    println("after optimization")

    println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)


    //////////////////////////////////////// Test Added Extra Physical Strategy /////////////////////////////////////////////////





    val tabA=sparkSession.range(20000000).as('a)
    val tabB=sparkSession.range(10000000).as('b)

    val result=tabA.join(tabB,  tabA("id")=== tabB("id"), "inner").groupBy().count()
    println(result.explain(extended = true).toString )




    val t1 = System.nanoTime

    result.show()

    val duration = (System.nanoTime - t1) / 1e9d

    println(duration)

    sparkSession.experimental.extraStrategies=IntervalJoin::Nil

    val result2=tabA.join(tabB,  tabA("id")=== tabB("id"), "inner").groupBy().count()
    println(result2.explain(extended = true).toString )
    val t2 = System.nanoTime

    result2.show()

    val duration2 = (System.nanoTime - t2) / 1e9d

    println(duration2)


  }

}
