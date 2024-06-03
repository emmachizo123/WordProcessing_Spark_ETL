
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, explode
import pyspark.sql.functions as f
import os
import psycopg2



def main( )-> None:
    """
        Script processes text file
        and converts them to pyspark  dataframes and stores them
        in an AWS PostreSQL database


        """

    # define the path to the JDBC driver
    """
    coding the path like below is important
    THe 'jdbc_driver_path' variable is set to the path of he JDBC driver JAR file
    Using 'os.path.jon' ensures that the path is constructed correctly regardless 
    of operating system
    os.path.dirname(__file__) => is the current location wherever that is
    '..','lib',"postgresql-42.7.3.jar"=> leads to the lib directory where the driver is
    """
    jdbc_driver_path = os.path.join(os.path.dirname(__file__), '..', 'lib', "postgresql-42.7.3.jar")

    print("printing the path to JDBC driver", jdbc_driver_path)

    # define the path to the file to read

    sourceFile_path = os.path.join(os.path.dirname(__file__), '..', 'FileStore', "WordData.txt")
    print("printing the path to word or text document to process",sourceFile_path)

    # Extract

    spark = SparkSession.builder.appName("Word Count Pipeline").config("spark.jars", jdbc_driver_path).getOrCreate()

    # I hard coded it before
    # spark = SparkSession.builder.appName("Word Count Pipeline").config("spark.jars","../lib/postgresql-42.7.3.jar").getOrCreate()

    df = spark.read.text(sourceFile_path)

    # df = spark.read.text("/FileStore/tables/WordData.txt")

    df.show()

    # COMMAND ----------

    """Transformation
    # we want to tranform the text file by creating a word count for all the words inside the text file
    # The Pyspark explode() function takes an iterable eg a list , dictionaries and creates a row from each element
    # to use explode on this text file we must first create a list...
    # a list of all the words in the text file since explode can
    # only take a list or dictionary or array or Json
    #then we can do a groupby and a count in that columns
    """

    # First, Lets use split function to create a list that holds each word in a row
    # Apply Split()

    df_splited = df.withColumn("splitedData", f.split("value", " "))
    print("print dataframe with a column  made of list of words created using f.spli() function")
    print("Note also that df.withColumn just adds a new column to the old dataframe")
    df_splited.show()

    # Apply Explode
    """
    Exploding the data
    explode() is a function from pyspark.sql.functions module. It transforms each elemnt
     of an array or each key-value pair of a dictionary into a separate row
     when used on a array column, it creates a new row for each elemnt
     of the array
     Also any time you use withColumn you are addding columns to the old dataframe

    """
    df_exploded = df_splited.withColumn("words", explode("splitedData"))  # new column called words
    print("Printing Dataframe with a new column 'words' created by exploding the column with splited data")
    df_exploded.show()

    """
    Lets extract the words column from the dataframe
    we use select to do that.
    when you use select instead of withcolumn you are creating a new Dataframe using the columns you selected
    """

    df_Words = df_exploded.select("words")
    print("printing dataframe that contains a single column called words")
    df_Words.show()

    """
    lets get the word count by doing a groupby and then  call the count(). Remember GroupBy must
    have an aggregate function
    """

    df_WordCount = df_Words.groupBy("words").count()
    print("printing amount of times each word appeared . we used groupby/count words")
    df_WordCount.show()

    # COMMAND ----------

    # Load

    driver = "org.postgresql.Driver"
    #url = "jdbc:postgresql://database-3.cte0iogswxo4.eu-north-1.rds.amazonaws.com/"
    url = "jdbc:postgresql://database-1.c5oa4i6owxbm.eu-north-1.rds.amazonaws.com/"
    # table = "emma_schema_pyspark.WordCountTable"
    table = "my_etl_schema_pyspark.WordCountTable1"
    user = "postgres"
    password = "chizoba123#"

    port = "5432"

    # Establish connection to PostgreSQL

    conn = psycopg2.connect(
        host="database-1.c5oa4i6owxbm.eu-north-1.rds.amazonaws.com",
        #host="database-3.cte0iogswxo4.eu-north-1.rds.amazonaws.com",
        port=port,
        dbname="postgres",
        user="postgres",
        password="chizoba123#"

    )
    print("printing connection", conn)
    ### SET AUTOCOMMIT TO true so whne table is dropped it is commited
    ### I was still getting error that table still exist even when it has been dropped
    # query had nt been committed

    conn.autocommit = True
    # create cursor object
    cur = conn.cursor()

    # drop the table if it exist
    drop_table_query = f"DROP TABLE IF EXISTS {table};"
    cur.execute(drop_table_query)

    # close the cursor and connection
    cur.close()
    conn.close()

    ####### Write DataFrame to PostgreSQL

    df_WordCount.write.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table).option(
        "mode", "overwrite").option("user", user).option("password", password).save()


if __name__ == '__main__':
    main()

