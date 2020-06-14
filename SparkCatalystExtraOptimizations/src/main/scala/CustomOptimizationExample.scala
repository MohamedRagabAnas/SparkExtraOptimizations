import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * User Defined Optimization
  */
object CustomOptimizationExample {

  object MultiplyOptimizationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case Multiply(left,right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
        println("optimization of one applied")
        left
    }
  }



}