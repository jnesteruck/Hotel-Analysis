from pyspark.sql import SparkSession
import os, time, subprocess

spark = SparkSession.builder \
        .master("local") \
        .appName("hotel_bookings") \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

def exists(file: str) -> bool:
    proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', file])
    proc.communicate()
    if proc.returncode != 0:
        return False
    return True

path = "/home/jnest/"

# Search for file in HDFS
os.system("clear")
print("Searching for hotel_bookings.csv in HDFS...")
time.sleep(2)

# Copy file to HDFS if not found
if not exists(path + "hotel_bookings.csv"):
    print("Couldn't find hotel_bookings.csv in HDFS location. Copying from local system...")
    os.system(f'hdfs dfs -put archive/hotel_bookings.csv {path}')
    if exists(path + "hotel_bookings.csv"):
        print("Successfully copied hotel_bookings.csv to HDFS...", end="")

# Load DataFrame from file
hotel_bookings = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "hotel_bookings.csv")

# Create temp view for SparkSQL queries
hotel_bookings.createOrReplaceTempView("hotel_bookings")

no = {'n', 'no'}
yes = {'y', 'yes'}
quit = {'q', 'quit'}

while True:
    query = "SELECT "
    col_sel = ""
    end = " FROM hotel_bookings"
    first = True
    manual = False
    while True:
        os.system("clear")
        print("What columns would you like to select? Enter --m to write your query manually")
        column = (input("\n>>> "))
        if column == "--m":
            manual = True
            break
        if not first:
            col_sel += ", "
        if first:
            first = False
        col_sel += column
        os.system("clear")
        print(query + col_sel + end)
        print("Would you like to select another column?\n")
        print("\tYes - Y")
        print("\tNo - N")
        choice = input("\n>>> ").lower()
        if choice in yes:
            continue
        else:
            break
    if manual:
        os.system("clear")
        print("Please enter your query in the space provided below.\nWARNING: Query statement cannot contain line breaks.")
        query = input("\nQUERY: ")
    else:
        os.system("clear")
        print("Do you want to filter the hotel_booking table? If so, enter the WHERE statement(s) below.\nIf multiple, seperate with AND.")
        where = input("\n>>> ")
        if where:
            where = " WHERE " + where
        os.system("clear")
        print("Do you want to group the by data by a column or columns? If so, enter the column(s) below.\nIf multiple, seperate by comma and space (col1, col2, etc.).")
        groupBy = input("\n>>> ")
        if groupBy:
            groupBy = " GROUP BY " + groupBy
        os.system("clear")
        print("Do you want to filter by your selected columns? If so, enter the HAVING statements below.\nIf multiple, seperate with AND.")
        having = input("\n>>> ")
        if having:
            having = " HAVING " + having
        os.system("clear")
        print("Do you want to sort your results by a column or columns? If so, enter the column(s) below.\nIf multiple, seperate by comma and space (col1, col2, etc.).")
        orderBy = input("\n>>> ")
        if orderBy:
            orderBy = " ORDER BY " + orderBy
    
        query += col_sel + end + where + groupBy + having + orderBy
        
    os.system("clear")
    print(query)
    print("Are you satisfied with the above query (Y/N)?")
    choice = input("\n>>> ").lower()
    if choice in yes:
        pass
    else:
        continue

    os.system("clear")
    print("Running query...")
    df = spark.sql(query)
    os.system("clear")
    print("Produced DataFrame from query. Would you like to preview it?")
    choice = input("\n>>> ").lower()
    if choice in yes:
        os.system("clear")
        df.show()
        time.sleep(2)
    
    print("\nWould you like to save this DataFrame (Y/N)?")
    choice = input("\n>>> ").lower()
    if choice in yes:
        # SAVE FILES
        while True:
            os.system("clear")
            print("Would you like to save a copy to HDFS, local storage, or both?")
            print("\t1 - HDFS only\n\t2 - Local storage only\n\t3 - Both\nQ - Quit")
            choice = input("\n>>> ").lower()
            q = False
            if choice in quit:
                q = True
                break
            try:
                choice = int(choice)
                if choice not in {1,2,3}:
                    raise ValueError
            except ValueError:
                print("Please select a valid option...")
                time.sleep(2)
                continue
            break
        if q:
            break
        savepath = path + "hotel_data/"
        localsavepath = "file:/mnt/c/Users/jan94/OneDrive/Documents/code/Revature_Project/data/"
        os.system("clear")
        print("Please enter a name for your file.")
        name = input("\n>>> ")
        if choice in {1, 3}:
            df.write.mode("overwrite").parquet(savepath + name)
        if choice in {2, 3}:
            df.write.mode("overwrite").csv(localsavepath + name + "_CSV", header=True)
            df.write.mode("overwrite").parquet(localsavepath + name + "_PARQUET")
        
    os.system("clear")
    print("Would you like to run another query (Y/N)? Selecting No will quit the program.")
    choice = input("\n>>> ").lower()
    if choice in no:
        break
    else:
        continue

# Ask to remove file to conserve space
while True:
    os.system("clear")
    print("Remove hotel-bookings.csv from HDFS to save space? (Y/N)")
    choice = input("\n>>> ")
    if choice in yes:
        print("Removing hotel-bookings.csv from HDFS...")
        os.system(f'hdfs dfs -rm {path}hotel_bookings.csv')
        print("Removed file from HDFS.")
        break
    elif choice in no:
        break
    else:
        print("Please select a valid option.")
        time.sleep(2)
        
spark.stop()