import org.apache.spark.sql.SparkSession

object LongIDGenerator {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("LogFileListIDGenerator")
      .getOrCreate()


    val generateLongId = () => {
      java.util.UUID.randomUUID().getMostSignificantBits() & java.lang.Long.MAX_VALUE
    }
    spark.udf.register("generateLongIdUDF", generateLongId)

    val logFileListDF = spark.read.parquet("s3a://the_parquet_file_location/")
    logFileListDF.count()
    logFileListDF.show()
    logFileListDF.createOrReplaceTempView("the_parquet_file_location")

    val logFileListDFWithID = spark.sql("select *, generateLongIdUDF() as ID from the_parquet_file_location")
    logFileListDFWithID.write.parquet("s3a://the_parquet_file_location2/")

    val logFileListDFWithIDResult = spark.read.parquet("s3a://the_parquet_file_location2/")
    logFileListDFWithIDResult.count()
    logFileListDFWithIDResult.show()

    logFileListDFWithIDResult.createOrReplaceTempView("the_parquet_file_location2")
    spark.sql("select ID,count(ID) as COUNT from the_parquet_file_location2 group by ID having COUNT>1").show()
  }
}
