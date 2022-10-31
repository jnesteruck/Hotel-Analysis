from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local") \
        .appName("hotel_bookings") \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

path = "/home/jnest/hotel_bookings.csv"

hotel_bookings = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path)

hotel_bookings.createOrReplaceTempView("hotel_bookings")

# spark.sql("SELECT hotel, CONCAT(arrival_date_year, '-', arrival_date_month, '-', arrival_date_day_of_month) AS arrival_date, "
#           "(stays_in_weekend_nights + stays_in_week_nights) AS stay, adults, children, babies, country FROM hotel_bookings").show()

'''byMonth = spark.sql("SELECT arrival_date_month AS month, SUM(IF(is_canceled=0, 1, 0)) AS stays, "
                    "SUM(IF(is_canceled=1, 1, 0)) AS cancellations, SUM(IF(is_canceled=1, 1, 0))"
                    "/(SUM(IF(is_canceled=0, 1, 0)) + SUM(IF(is_canceled=1, 1, 0))) AS cancellationRate "
                    "FROM hotel_bookings GROUP BY arrival_date_month ORDER BY stays DESC")

byYear = spark.sql("SELECT arrival_date_year AS year, SUM(IF(is_canceled=0, 1, 0)) AS stays, "
                   "SUM(IF(is_canceled=1, 1, 0)) AS cancellations, SUM(IF(is_canceled=1, 1, 0))"
                   "/(SUM(IF(is_canceled=0, 1, 0)) + SUM(IF(is_canceled=1, 1, 0))) AS cancellationRate "
                   "FROM hotel_bookings GROUP BY arrival_date_year ORDER BY year")

byWeek = spark.sql("SELECT arrival_date_week_number AS week, SUM(IF(is_canceled=0, 1, 0)) AS stays, "
                   "SUM(IF(is_canceled=1, 1, 0)) AS cancellations, SUM(IF(is_canceled=1, 1, 0))"
                   "/(SUM(IF(is_canceled=0, 1, 0)) + SUM(IF(is_canceled=1, 1, 0))) AS cancellationRate "
                   "FROM hotel_bookings GROUP BY arrival_date_week_number ORDER BY week")

spark.sql("SELECT *, TO_DATE(CONCAT(arrival_date_year, '-', arrival_date_month, '-', IF("
              "arrival_date_day_of_month<10, CONCAT('0', arrival_date_day_of_month), CAST(arrival_date_day_of_month AS STRING))"
          "), 'yyyy-MMMM-dd') AS arrival_date FROM hotel_bookings").createOrReplaceTempView("withDate")

byDate = spark.sql("SELECT arrival_date AS date, SUM(IF(is_canceled=0, 1, 0)) AS stays, "
                   "SUM(IF(is_canceled=1, 1, 0)) AS cancellations, SUM(IF(is_canceled=1, 1, 0))"
                   "/(SUM(IF(is_canceled=0, 1, 0)) + SUM(IF(is_canceled=1, 1, 0))) AS cancellationRate "
                   "FROM withDate GROUP BY arrival_date ORDER BY date")

stayTotals = spark.sql("SELECT SUM(IF(is_canceled=0, 1, 0)) AS total_stays, "
                       "SUM(IF(is_canceled=1, 1, 0)) AS cancellations FROM hotel_bookings")'''

# SAVE FILES
savepath = "file:/mnt/c/Users/jan94/OneDrive/Documents/code/Revature_Project/data/"
# byMonth.write.csv(savepath + "byMonth", header=True)
# byYear.write.csv(savepath + "byYear", header=True)
# byWeek.write.csv(savepath + "byWeek", header=True)
# byDate.write.csv(savepath + "byDate", header=True)

spark.stop()