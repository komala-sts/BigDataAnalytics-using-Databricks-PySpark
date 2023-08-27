-- Databricks notebook source
-- MAGIC %python
-- MAGIC #3 versions of ClinicalTrial data for 2019, 2020 and 2021 are used for further analysis in SPARK SQL.
-- MAGIC #SQL: Data Cleaning - Step 1 : Cleaning the DBFS from the csv files that the notebook needs to create again.
-- MAGIC dbutils.fs.rm("/FileStore/tables/pharma.csv", True ) 
-- MAGIC dbutils.fs.rm("/FileStore/tables/clinicaltrial_2019.csv", True ) 
-- MAGIC dbutils.fs.rm("/FileStore/tables/clinicaltrial_2020.csv", True ) 
-- MAGIC dbutils.fs.rm("/FileStore/tables/clinicaltrial_2021.csv", True ) 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL: Data Cleaning - Step 2 : Cleaning the Local(temporary) file system from the zip files(Datasets) that are used by the notebook again.
-- MAGIC dbutils.fs.rm("file:/tmp/pharma.zip", True ) 
-- MAGIC dbutils.fs.rm("file:/tmp/clinicaltrial_2019.zip", True ) 
-- MAGIC dbutils.fs.rm("file:/tmp/clinicaltrial_2020.zip", True )
-- MAGIC dbutils.fs.rm("file:/tmp/clinicaltrial_2021.zip", True )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL: Data Preparation - Step 1 : Zip(.zip) Files are copied from DataBricks FileSystem(DBFS) to the Local filesystem to unzip
-- MAGIC dbutils.fs.cp("FileStore/tables/clinicaltrial_2019.zip", "file:/tmp/")
-- MAGIC dbutils.fs.cp("FileStore/tables/clinicaltrial_2020.zip", "file:/tmp/")
-- MAGIC dbutils.fs.cp("FileStore/tables/clinicaltrial_2021.zip", "file:/tmp/")
-- MAGIC dbutils.fs.cp("FileStore/tables/pharma.zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL: Data Preparation - Step 2 : Verifying the unzipped files' existence at the Local file system 
-- MAGIC dbutils.fs.ls("file:/tmp/")

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC unzip -d /tmp/ /tmp/clinicaltrial_2019.zip
-- MAGIC unzip -d /tmp/ /tmp/clinicaltrial_2020.zip
-- MAGIC unzip -d /tmp/ /tmp/clinicaltrial_2021.zip
-- MAGIC unzip -d /tmp/ /tmp/pharma.zip
-- MAGIC ls /tmp/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL: Data Preparation - Step 4 : unzipped(.csv) Files are moved back to DBFS from Local FileSystem
-- MAGIC dbutils.fs.mv("file:/tmp/clinicaltrial_2019.csv" , "FileStore/tables/", True)
-- MAGIC dbutils.fs.mv("file:/tmp/clinicaltrial_2020.csv" , "FileStore/tables/", True)
-- MAGIC dbutils.fs.mv("file:/tmp/clinicaltrial_2021.csv" , "FileStore/tables/", True)
-- MAGIC dbutils.fs.mv("file:/tmp/pharma.csv" , "FileStore/tables/", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL: Data Preparation - Step 5 :  Verifying the existence of the copied files at the DataBricks FileSystem
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import* 
-- MAGIC #3 versions of ClinicalTrial data for 2019, 2020 and 2021 are used for further analysis in SPARK SQL.
-- MAGIC #Loading the data from the 4 CSV files to Spark DataFrames using spark.read.csv method() with specifications to infer Schema automatically, existence of header and delimiter-symbol 
-- MAGIC sql_ct19df = spark.read.csv("/FileStore/tables/clinicaltrial_2019.csv", header ="true", inferSchema="true", sep="|")
-- MAGIC sql_ct20df = spark.read.csv("/FileStore/tables/clinicaltrial_2020.csv", header ="true", inferSchema="true", sep="|")
-- MAGIC sql_ct21DF = spark.read.csv("/FileStore/tables/clinicaltrial_2021.csv", header ="true", inferSchema="true", sep="|")
-- MAGIC sql_pharmaDF = spark.read.csv("/FileStore/tables/pharma.csv", header ="true", inferSchema="true", sep=",")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Viewing first 15 rows contents of the sql_ct21DF Dataframe (ClinicalTrial_2021.csv) using display() method
-- MAGIC display(sql_ct21DF.limit(15))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Printing the Schema of sql_ct21DF Dataframe
-- MAGIC sql_ct21DF.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Viewing first 15 rows contents of the sql_pharmaDF using show() method
-- MAGIC sql_pharmaDF.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Printing the Schema of sql_pharmaDF Dataframe
-- MAGIC sql_pharmaDF.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Creating the Spark SQL views for all 4 Datasets using createOrReplaceTempView() method
-- MAGIC sql_ct21DF.createOrReplaceTempView("SQL_CT21View")
-- MAGIC sql_pharmaDF.createOrReplaceTempView("SQL_PharmaView")
-- MAGIC sql_ct19df.createOrReplaceTempView("sql_ct19View")
-- MAGIC sql_ct20df.createOrReplaceTempView("sql_ct20View")

-- COMMAND ----------

--SQL: Viewing few contents of the ClinicalTrial's SQL View
SELECT * FROM SQL_CT21View LIMIT 10;

-- COMMAND ----------

-- SQL Question# 1 FOR 2021 Finding Distinct studies by computing aggregate method count() on distinct ID column values
SELECT count(DISTINCT(ID)) AS Distinct_Studies FROM SQL_CT21View ;

-- COMMAND ----------

-- SQL Question# 2 FOR 2021
SELECT Type, count(Type) AS TypeCount2021 FROM SQL_ct21view GROUP BY Type ORDER BY TypeCount2021 DESC;

-- COMMAND ----------

--SQL QUESTION#3 : Finding the top 5 most common Conditions in ClinicalTrial_2021    
CREATE OR REPLACE TEMP VIEW SQL_CT21_Conditions_SplitView AS SELECT split(Conditions,',') as Conditions2021 from SQL_CT21view;

-- COMMAND ----------

SELECT * FROM SQL_CT21_Conditions_SplitView LIMIT 10;

-- COMMAND ----------

--explode function is used to create or split an array to rows
CREATE OR REPLACE TEMP VIEW Exploded_Conditions_21 AS SELECT explode(Conditions2021) AS Conditions2021 FROM SQL_CT21_Conditions_SplitView;

-- COMMAND ----------

SELECT * FROM Exploded_Conditions_21 LIMIT 10;

-- COMMAND ----------

SELECT Conditions2021, Count(*) AS Conditions_Count_2021  from Exploded_Conditions_21 GROUP BY Conditions2021 ORDER BY Conditions_Count_2021 DESC LIMIT 5;

-- COMMAND ----------

-- SQL QUESTION# 4 FOR 2021 : Used Left Outer Join with GroupBy and Subquery Concept. Joining SPONSOR of SQL_CT21view and PARENT_COMPANY of SQL_PharmaView grouped by SPONSOR to get final result. Finally, sub-queried to filter SPONSOR Not in SQL_PharmaView 
SELECT C.SPONSOR AS Non_PharmaSponsors, COUNT(C.SPONSOR) AS Non_PharmaSponsors_Count2021 from SQL_CT21view C left outer join SQL_PharmaView P ON C.SPONSOR = P.PARENT_COMPANY GROUP BY C.SPONSOR HAVING C.SPONSOR NOT IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) ORDER BY NON_PharmaSponsors_Count2021 DESC LIMIT 10;

-- COMMAND ----------

--SQL: QUESTION# 5 FOR 2021: Implemented using Common Table Expression-Subquery concept. CT52021_result contains Final-Analysis. COMPLETION column contains Eg:(Jan 232). Month alone extracted as 'COMPLETEDSTUDIES_MONTH_2021' using substring() function. To display proper monthly order, numeric values are assigned to extracted(using substring() function) first 3-characters of COMPLETION column. Named as 'MonthNum'. Also, Filtered the COMPLETION column by 2021 and grouped on Month(COMPLETEDSTUDIES_MONTH_2021).
CREATE OR REPLACE TEMP VIEW CT52021_result as  WITH CT2021 (STATUS, COMPLETION, COMPLETEDSTUDIES_MONTH_2021, MonthNum, COMPLETEDSTUDIES_2021)
AS
(SELECT STATUS,COMPLETION, SUBSTRING(COMPLETION,1,3) AS COMPLETEDSTUDIES_MONTH_2021, CASE WHEN SUBSTRING(COMPLETION,1,3)='Jan' then 1 WHEN SUBSTRING(COMPLETION,1,3)='Feb' then 2 WHEN SUBSTRING(COMPLETION,1,3)='Mar' then 3 WHEN SUBSTRING(COMPLETION,1,3)='Apr' then 4 WHEN SUBSTRING(COMPLETION,1,3)='May' then 5 WHEN SUBSTRING(COMPLETION,1,3)='Jun' then 6 WHEN SUBSTRING(COMPLETION,1,3)='Jul' then 7 WHEN SUBSTRING(COMPLETION,1,3)='Aug' then 8 WHEN SUBSTRING(COMPLETION,1,3)='Sep' then 9 WHEN SUBSTRING(COMPLETION,1,3)='Oct' then 10 WHEN SUBSTRING(COMPLETION,1,3)='Nov' then 11 WHEN SUBSTRING(COMPLETION,1,3)='Dec' then 12 else '' end as  MonthNum, COUNT(SUBSTRING(COMPLETION,1,3)) AS COMPLETEDSTUDIES_2021 FROM SQL_CT21view GROUP BY STATUS, COMPLETION, COMPLETEDSTUDIES_MONTH_2021 HAVING STATUS='Completed' AND COMPLETION  LIKE '%2021' ORDER BY int(MonthNum) )
SELECT  COMPLETEDSTUDIES_MONTH_2021, COMPLETEDSTUDIES_2021 FROM CT2021;

-- COMMAND ----------

SELECT * FROM CT52021_result;

-- COMMAND ----------

--SQL_Q5: Step2: CREATING A TABLE FOR EXPORTING DATA TO POWERBI FOR VISUALIZATION IMPLEMENTATION FOR QUESTION 5 ANALYSIS
CREATE OR REPLACE TABLE CTQ5SQL_2021_PBI as  WITH CT2021 (STATUS, COMPLETION, COMPLETEDSTUDIES_MONTH_2021, MonthNum, COMPLETEDSTUDIES_2021)
AS
(SELECT STATUS,COMPLETION, SUBSTRING(COMPLETION,1,3) AS COMPLETEDSTUDIES_MONTH_2021, CASE WHEN SUBSTRING(COMPLETION,1,3)='Jan' then 1 WHEN SUBSTRING(COMPLETION,1,3)='Feb' then 2 WHEN SUBSTRING(COMPLETION,1,3)='Mar' then 3 WHEN SUBSTRING(COMPLETION,1,3)='Apr' then 4 WHEN SUBSTRING(COMPLETION,1,3)='May' then 5 WHEN SUBSTRING(COMPLETION,1,3)='Jun' then 6 WHEN SUBSTRING(COMPLETION,1,3)='Jul' then 7 WHEN SUBSTRING(COMPLETION,1,3)='Aug' then 8 WHEN SUBSTRING(COMPLETION,1,3)='Sep' then 9 WHEN SUBSTRING(COMPLETION,1,3)='Oct' then 10 WHEN SUBSTRING(COMPLETION,1,3)='Nov' then 11 WHEN SUBSTRING(COMPLETION,1,3)='Dec' then 12 else '' end as  MonthNum, COUNT(SUBSTRING(COMPLETION,1,3)) AS COMPLETEDSTUDIES_2021 FROM SQL_CT21view GROUP BY STATUS, COMPLETION, COMPLETEDSTUDIES_MONTH_2021 HAVING STATUS='Completed' AND COMPLETION  LIKE '%2021' ORDER BY int(MonthNum) )
SELECT  COMPLETEDSTUDIES_MONTH_2021, COMPLETEDSTUDIES_2021 FROM CT2021;

-- COMMAND ----------

SELECT * FROM CTQ5SQL_2021_PBI;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL_Q5: Step4: CREATING DATAFRAME FOR GENERATING VISUALIZATION USING MATPLOTLIB PYTHON LIBRARYS
-- MAGIC from pyspark.sql.types import* 
-- MAGIC CT52021_DF = spark.sql("select * from CT52021_result")
-- MAGIC #display(CT52021_DFvisualization)
-- MAGIC CT52021_DF.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL_Q5: Step5:Visualization for SQL Question 5 using matplotlib
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC plt.style.use('ggplot')
-- MAGIC CT52021_DF_PD_vis = CT52021_DF.toPandas()
-- MAGIC fig, ax = plt.subplots()
-- MAGIC x = CT52021_DF_PD_vis['COMPLETEDSTUDIES_MONTH_2021']
-- MAGIC y = CT52021_DF_PD_vis['COMPLETEDSTUDIES_2021']
-- MAGIC ax.bar(x, y, width=0.8, edgecolor="white", linewidth=0.7)
-- MAGIC ax.set_title('Bar Chart Dipicts the Month Wise Completed Studies for the Year 2021 of Clinical Trial')
-- MAGIC plt.show()

-- COMMAND ----------

----SQL Question# 6 FURTHER ANALYSIS 1 - Top 10 most common interventions of Pharma Sponsors for ALL 3 years - 2019,2020 and 2021
SELECT  Sponsor AS Pharma_Sponsor, Interventions,SUM(CASE WHEN CT_YEAR="2019" THEN Count_Interventions END) AS Interventions_2019, SUM(CASE WHEN CT_YEAR="2020" THEN Count_Interventions END) AS Interventions_2020, SUM( CASE WHEN CT_YEAR="2021" THEN Count_Interventions END) AS Interventions_2021 FROM (
(SELECT Sponsor, Interventions,'2019' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT19view group by Sponsor,Interventions,CT_YEAR having Sponsor IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10)
UNION
(SELECT Sponsor, Interventions,'2020' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT20view group by Sponsor,Interventions,CT_YEAR having Sponsor IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10 )
UNION
(SELECT Sponsor, Interventions,'2021' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT21view group by Sponsor,Interventions,CT_YEAR having Sponsor IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10 )) RESULT  GROUP BY SPONSOR, INTERVENTIONS   ORDER BY 3 DESC,4 DESC, 5 DESC LIMIT 10 ;

-- COMMAND ----------

--SQL Q#6 FA 1: TABLE CREATION For Implementing Visualization in POWER BI
CREATE OR REPLACE TABLE SQL_PHARMA_INT_3YRS  AS SELECT Sponsor AS Pharma_Sponsor, Interventions,SUM(CASE WHEN CT_YEAR="2019" THEN Count_Interventions END) AS Interventions_2019, SUM(CASE WHEN CT_YEAR="2020" THEN Count_Interventions END) AS Interventions_2020, SUM( CASE WHEN CT_YEAR="2021" THEN Count_Interventions END) AS Interventions_2021 FROM (
(SELECT Sponsor, Interventions,'2019' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT19view group by Sponsor,Interventions,CT_YEAR having Sponsor IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10)
UNION
(SELECT Sponsor, Interventions,'2020' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT20view group by Sponsor,Interventions,CT_YEAR having Sponsor IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10 )
UNION
(SELECT Sponsor, Interventions,'2021' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT21View group by Sponsor,Interventions,CT_YEAR having Sponsor IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10 )) RESULT  GROUP BY SPONSOR, INTERVENTIONS   ORDER BY 3 DESC,4 DESC, 5 DESC LIMIT 10 ;

-- COMMAND ----------

SELECT * FROM SQL_PHARMA_INT_3YRS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL Q#6 FA1 - Dataframe creation for matplotlib Visualization Implementation 
-- MAGIC sql_query= """ SELECT * FROM SQL_PHARMA_INT_3YRS;"""
-- MAGIC sqlQ6FAPlotDF = spark.sql(sql_query).toPandas()
-- MAGIC display(sqlQ6FAPlotDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Ref: https://www.geeksforgeeks.org/how-to-plot-multiple-data-columns-in-a-dataframe/
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as mp
-- MAGIC PlotHis = pd.DataFrame(sqlQ6FAPlotDF, columns=["Sponsor", "Interventions", "Interventions_2019","Interventions_2020","Interventions_2021"])
-- MAGIC ax = PlotHis.plot(x="Sponsor", y=["Interventions", "Interventions_2019","Interventions_2020","Interventions_2021"], kind="bar", figsize=(9, 8))
-- MAGIC ax.set_xlabel("Pharmaceutical Sponsor")
-- MAGIC ax.set_ylabel("Interventions Count")
-- MAGIC ax.set_title("(Violation) Pharmaceutical Sponsors Top 10 most common Interventions Count for the Year 2019, 2020 and 2021") 
-- MAGIC #print Histogram graph
-- MAGIC mp.show()

-- COMMAND ----------

--SQL Q#7: FURTHER ANALYSIS 2: LIST OF TOP 10 most common interventions of Non-Pharma Sponsors for 2021
SELECT SPONSOR AS NONPHARMA_SPONSOR,Interventions, COUNT(Interventions) AS Interventions_2021 from SQL_CT21view GROUP BY SPONSOR,Interventions  HAVING SPONSOR NOT IN (SELECT PARENT_COMPANY FROM SQL_PharmaView )    ORDER BY Interventions_2021 DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL Q#7: Further Analysis 2: Dataframe creation for matplotlib Visualization Implementation 
-- MAGIC sql_query= """ SELECT SPONSOR AS NONPHARMA_SPONSOR,Interventions, COUNT(Interventions) AS Interventions_2021 from SQL_CT21view GROUP BY SPONSOR,Interventions  HAVING SPONSOR NOT IN (SELECT PARENT_COMPANY FROM SQL_PharmaView )    ORDER BY Interventions_2021 DESC LIMIT 10;"""
-- MAGIC SQLQ7FAPlotDF = spark.sql(sql_query).toPandas()
-- MAGIC #Q7PlotDF.createOrReplaceTempView("Q7temp_view")
-- MAGIC #Q7PlotDF = spark.sql("SELECT * FROM Q7temp_view").toPandas()
-- MAGIC display(SQLQ7FAPlotDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL Q#7: Further Analysis 2:  Top 10 most common Interventions of Non- pharmaceutical Sponsors
-- MAGIC #import Matplotlib, which will help us customize our plots further.
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import seaborn as sns
-- MAGIC #https://elitedatascience.com/python-seaborn-tutorial
-- MAGIC sns.set_style('whitegrid') 
-- MAGIC plt.figure(figsize=(12,12)) # Set plot dimensions 
-- MAGIC sns.catplot(data=SQLQ7FAPlotDF, kind='bar', x='Interventions', y='Interventions_2021', hue='NONPHARMA_SPONSOR')
-- MAGIC #To display the X labels vertically that is 90degree
-- MAGIC plt.xticks(rotation=90)
-- MAGIC plt.title('Non Pharmaceutical Sponsors Top 10 most common Interventions Count for the Year 2021 ')
-- MAGIC plt.show()

-- COMMAND ----------

--SQL table for visualization of Question 7 FURTHER ANALYSIS - LIST OF TOP 10 most common interventions of Pharma Spaonsors for 2021
CREATE OR REPLACE TABLE SQLNONPHARMA_INTERVENTION AS SELECT SPONSOR AS NONPHARMA_SPONSOR,Interventions, COUNT(Interventions) AS Interventions_2021 from SQL_CT21view GROUP BY SPONSOR,Interventions  HAVING SPONSOR NOT IN (SELECT PARENT_COMPANY FROM SQL_PharmaView )    ORDER BY Interventions_2021 DESC LIMIT 10;

-- COMMAND ----------

SELECT * FROM SQLNONPHARMA_INTERVENTION

-- COMMAND ----------

----SQL Question# 8 FURTHER ANALYSIS 3- TOP 10 most common interventions of Non-Pharma Sponsors for ALL 3 years - 2019,2020 and 2021. Implemented using Union operator and subquery
SELECT  Sponsor AS Non_Pharma_Sponsor, Interventions,SUM(CASE WHEN CT_YEAR="2019" THEN Count_Interventions END) AS Interventions_2019, SUM(CASE WHEN CT_YEAR="2020" THEN Count_Interventions END) AS Interventions_2020, SUM( CASE WHEN CT_YEAR="2021" THEN Count_Interventions END) AS Interventions_2021 FROM (
(SELECT Sponsor, Interventions,'2019' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT19view group by Sponsor,Interventions,CT_YEAR having Sponsor NOT IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10)
UNION
(SELECT Sponsor, Interventions,'2020' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT20view group by Sponsor,Interventions,CT_YEAR having Sponsor NOT IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10 )
UNION
(SELECT Sponsor, Interventions,'2021' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT21view group by Sponsor,Interventions,CT_YEAR having Sponsor NOT  IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10 )) RESULT  GROUP BY SPONSOR, INTERVENTIONS   ORDER BY 3 DESC,4 DESC, 5 DESC LIMIT 10 

-- COMMAND ----------

--SQL Question# 8 FURTHER ANALYSIS: 3 TABLE CREATION For generating Visualization in POWER BI
CREATE OR REPLACE TABLE SQL_NONPHARMA_INT_3YRS  AS  SELECT Sponsor AS Non_Pharma_Sponsor, Interventions,SUM(CASE WHEN CT_YEAR="2019" THEN Count_Interventions END) AS Interventions_2019, SUM(CASE WHEN CT_YEAR="2020" THEN Count_Interventions END) AS Interventions_2020, SUM( CASE WHEN CT_YEAR="2021" THEN Count_Interventions END) AS Interventions_2021 FROM (
(SELECT Sponsor, Interventions,'2019' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT19view group by Sponsor,Interventions,CT_YEAR having Sponsor NOT IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10)
UNION
(SELECT Sponsor, Interventions,'2020' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT20view group by Sponsor,Interventions,CT_YEAR having Sponsor NOT IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10 )
UNION
(SELECT Sponsor, Interventions,'2021' AS CT_YEAR, count(Interventions) as Count_Interventions FROM SQL_CT21view group by Sponsor,Interventions,CT_YEAR having Sponsor NOT  IN (SELECT PARENT_COMPANY FROM SQL_PharmaView ) order by Count_Interventions desc limit 10 )) RESULT  GROUP BY SPONSOR, INTERVENTIONS   ORDER BY 3 DESC,4 DESC, 5 DESC LIMIT 10 

-- COMMAND ----------

SELECT * FROM SQL_NONPHARMA_INT_3YRS;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #SQL: Question# 8 FURTHER ANALYSIS 3  - For Implementing Visualization using Python matplotlib Library
-- MAGIC sql_query= """ SELECT * FROM SQL_NONPHARMA_INT_3YRS;"""
-- MAGIC sqlQ8FA3PlotDF = spark.sql(sql_query).toPandas()
-- MAGIC display(sqlQ8FA3PlotDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as mp 
-- MAGIC PlotHis = pd.DataFrame(sqlQ8FA3PlotDF, columns=["Non_Pharma_Sponsor", "Interventions", "Interventions_2019","Interventions_2020","Interventions_2021"])
-- MAGIC # plot the dataframe
-- MAGIC ax = PlotHis.plot(x="Non_Pharma_Sponsor", y=["Interventions", "Interventions_2019","Interventions_2020","Interventions_2021"], kind="bar", figsize=(9, 8))
-- MAGIC ax.set_xlabel("Non-Violation Pharmaceutical Sponsor")
-- MAGIC ax.set_ylabel("Interventions Count")
-- MAGIC ax.set_title("Non-Violation Pharmaceutical Sponsors Top 10 most common Interventions Count for the Year 2019, 2020 and 2021") 
-- MAGIC # print the Histogram
-- MAGIC mp.show()

-- COMMAND ----------


