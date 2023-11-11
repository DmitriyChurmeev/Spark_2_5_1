import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, round, when}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object App {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .appName("Praktik-2.5.1")
      .master("local")
      .getOrCreate()

    val mallCustomersCsvSchema = StructType(Seq(
      StructField("CustomerID", LongType),
      StructField("Gender", StringType),
      StructField("Age", LongType),
      StructField("Annual Income (k$)", LongType),
      StructField("Spending Score (1-100)", LongType)
    ))

    val incomeDF = spark.read
      .format("csv")
      .schema(mallCustomersCsvSchema)
      .option("header", "true")
      .option("sep", ",")
      .load("src/main/resources/mall_customers.csv")
      .withColumn("Age", col("Age") + 2)
      .filter(col("Age") >= 30 && col("Age") <= 35)
      .groupBy("Gender", "Age")
      .agg(round(avg("Annual Income (k$)"), 1).as("averageOfAnnualIncome"))
      .orderBy("Gender", "Age")
      .withColumn("gender_code", when(col("Gender") === "Male", 1)
        .when(col("Gender") === "Female", 0))

    incomeDF.write
      // Предпочитаю явно указывать тип файла
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/customers")
  }

}
