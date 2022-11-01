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

########## BOOKINGS/CANCELLATIONS BY DATE/TIME OF YEAR ##########

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

########## Data by Date ##########

########## Data By Hotel Type ##########

########## Data By Customer Type ##########

########## Data By Country ##########
byCountry = spark.sql("SELECT country, SUM(IF(is_canceled=0, 1, 0)) AS stays, SUM(IF(is_canceled=1, 1, 0)) AS cancellations, SUM(IF(is_canceled=1, 1, 0))/"
                      "(SUM(IF(is_canceled=0, 1, 0)) + SUM(IF(is_canceled=1, 1, 0))) AS cancellationRate, SUM(required_car_parking_spaces) "
                      "AS total_parking_required, SUM(total_of_special_requests) AS total_special_requests, SUM(IF(hotel='City Hotel', 1, 0)) AS city_hotels, "
                      "SUM(IF(hotel='Resort Hotel', 1, 0)) AS resort_hotels, SUM(IF(customer_type='Transient', 1, 0)) AS transients, "
                      "SUM(IF(customer_type='Transient-Party', 1, 0)) AS transient_parties, SUM(IF(customer_type='Contract', 1, 0)) AS contracts, "
                      "SUM(IF(customer_type='Transient' OR customer_type='Transient-Party' OR customer_type='Contract', 0, 1)) AS others, "
                      "SUM(adults) AS adults, SUM(children) AS children, SUM(babies) AS babies "
                      "FROM hotel_bookings GROUP BY country ORDER BY stays DESC")

datesByCountry = spark.sql("SELECT country, SUM(IF(arrival_date_year=2015, 1, 0)) AS a2015, SUM(IF(arrival_date_year=2016, 1, 0)) AS a2016, "
                           "SUM(IF(arrival_date_year=2017, 1, 0)) AS a2017, SUM(IF(arrival_date_month='January', 1, 0)) AS January, "
                           "SUM(IF(arrival_date_month='February', 1, 0)) AS February, SUM(IF(arrival_date_month='March', 1, 0)) AS March, "
                           "SUM(IF(arrival_date_month='April', 1, 0)) AS April, SUM(IF(arrival_date_month='May', 1, 0)) AS May, "
                           "SUM(IF(arrival_date_month='June', 1, 0)) AS June, SUM(IF(arrival_date_month='July', 1, 0)) AS July, "
                           "SUM(IF(arrival_date_month='August', 1, 0)) AS August, SUM(IF(arrival_date_month='September', 1, 0)) AS September, "
                           "SUM(IF(arrival_date_month='October', 1, 0)) AS October, SUM(IF(arrival_date_month='November', 1, 0)) AS November, "
                           "SUM(IF(arrival_date_month='December', 1, 0)) AS December FROM hotel_bookings GROUP BY country ORDER BY a2016 DESC")

byCountry.show()
datesByCountry.show()

# SAVE FILES
savepath = "file:/mnt/c/Users/jan94/OneDrive/Documents/code/Revature_Project/data/"
# byMonth.write.csv(savepath + "byMonth", header=True)
# byYear.write.csv(savepath + "byYear", header=True)
# byWeek.write.csv(savepath + "byWeek", header=True)
# byDate.write.csv(savepath + "byDate", header=True)

spark.stop()