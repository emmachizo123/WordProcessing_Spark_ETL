
Brief Summary
This Application is an ETL pipleline that reads in a word document/Text file of any size
and transforms the text file 
into a pyspark dataframe of word counts. 
The PySpark dataframe is then written to a table
in an AWS (RDS) PostreSQL database

Installation
To install the project simply clone the repository and install the dependencies using 
the following steps:

1. 	git clone https://github.com/emmachizo123/WordProcessing_Spark_ETL
2. 	cd WordProcessing_Spark_ETL
3. 	pip install -r requirements.txt


Usage
To run the application:
Use the following steps:
Open a command line prompt
Change directory to WordProcessing_Spark_ETL folder\src
From this directory run:
python main.py

