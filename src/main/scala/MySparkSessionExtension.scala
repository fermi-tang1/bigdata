import org.apache.spark.sql.SparkSessionExtensions

/**
 * @Author: Fermi.Tang
 * @Date: Created in 15:53,2022/9/1
 */
class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule {
      session => CustomRule(session)
    }
  }
}
