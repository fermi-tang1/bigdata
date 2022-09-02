import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * @Author: Fermi.Tang
 * @Date: Created in 15:53,2022/9/1
 */
case class CustomRule(spark: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Sort(_, _, child) if child.maxRows.exists(_ <= 1L) => {
      print("The maximum number of rows are less than 1, no need to sort!!!")
      child
    }
    case other => {
      logWarning(s"Optimization batch is excluded from the CustomRule optimizer")
      other
    }
  }
}
