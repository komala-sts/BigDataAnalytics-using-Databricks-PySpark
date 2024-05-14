
# MSc Data Science Coursework on Big Data Tools and Technics using DataBricks.<br/>
<h3>1 Tasks</h3>
2 tasks were given with separate datasets a set of problem statements for each task. Required to implement your solution to each problem based on tasks’ description.
<h4>2.1. Task 1 </h4>
<p align='justify'>You will be using clinical trial datasets in this work and combining the information with a list of pharmaceutical companies. You will be given the answers to the questions, for a basic implementation, for two historical datasets, so you can verify your basic solution to the problems. Your final submission will need to consist of results executed on the third, 2021, release of the data. All data will be available from Blackboard.</p>
<b>2.1.1. Datasets:</b>
The data necessary for this assignment will be downloadable as .csv files. The .csv files have a header describing the file’s contents. They are:<br/>
<b>1. Clinicaltrial_<year>.csv:</b><br/>
<p align='justify'>Each row represents an individual clinical trial, identified by an Id, listing the sponsor (Sponsor), the status of the study at time of the file’s download (Status), the start and completion dates (Start and Completion respectively), the type of study (Type), when the trial was first submitted (Submission), and the lists of conditions the trial concerns (Conditions) and the interventions explored (Interventions). Individual conditions and interventions are separated by commas.
(Source: ClinicalTrials.gov)</p>
<b>2. pharma.csv:</b>
<p align='justify'>The file contains a small number of a publicly available list of pharmaceutical violations. For the purposes of this work, we are interested in the second column, Parent Company, which contains the name of the pharmaceutical company in question.
(Source: https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals)
When creating tables for this work, you must name them as follows:<br/>
<ul>
  <li>clinicaltrial_2021 (and clinicaltrial_2019, clinicaltrial_2020 for the sample data)</li>
  <li>pharma</li>
</ul>
The uploaded datasets, must exist (and be named) in the following locations:<br/>

/FileStore/tables/clinicaltrial_2021.csv (similarly for sample data)<br/>

/FileStore/tables/pharma.csv<br/>

<p align='justify'>This is to ensure that we can run your notebooks when testing your code (marks are allocated for your code running).
You are to implement all steps 3 times: once in Spark SQL and twice in PySpark (For RDD and DataFrame).
For the visualisation of the results, you are free to use any tool that fulfils the requirements, which can be tools such as Python’s matplotlib, Excel, Power Bi, Tableau, or any other free open-source tool you may find suitable. Using built-in visualizations directly is permitted, it will however not yield a high number of marks. Your report needs to state the software used to generate the visualization, otherwise a built-in visualization will be assumed.</p>
<b>2.1.2. Problem statement</b><br/>
You are a data analyst / data scientist whose client wishes to gain further insight into clinical trials. You are tasked with answering these questions, using visualisations where these would support your conclusions.<br/>
You should address the following questions. You should use the solutions for historical datasets (available on Blackboard) to test your implementation.<br/>
1. The number of studies in the dataset. You must ensure that you explicitly check distinct studies.<br/>
2. You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent.<br/>
3. The top 5 conditions (from Conditions) with their frequencies.<br/>
4. Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the Parent Company column contains all possible pharmaceutical companies.<br/>
5. Plot number of completed studies each month in a given year – for the submission dataset, the year is 2021. You need to include your visualization as well as a table of all the values you have plotted for each month.<br/>
<b>2.1.3. Extra features to be implemented for extra marks</b>
If you answer correctly all 5 problems, you will get a “merit” mark for this task (maximum 69% percent of the task mark). You will get more marks If you implement several extra features like follows (but are not limited to):<br/>

➢ Maximum 3 further analyses of the data, motivated by the questions asked (new problem statements other than the 5 problems)<br/>

➢ Writing general and reusable code. For example, ensuring that switching to a different version of the data requires only the change of one variable.<br/>

Using more advance methods to solve the problems like defining and using user-defined functions.<br/>

Successfully implementing Spark functions that you have not used in the workshop.<br/>

Creation of additional visualizations presenting useful information based on your own exploration which is not covered by the problem statements.<br/>

<h4>2.2. Task 2 </h4>

<b>2.2.1. Problem statement</b><br/>
<p align='justify'>For your second task, imagine you are working as a data scientist for a manufacturing company. They are using vibration sensors to monitor the machinery within their production line and want to use this data for predictive maintenance – the aim is to be able to classify whether there is a fault with the machine based on the readings from the vibration sensors. We have provided you with a dataset FaultDataset.csv. Each row contains twenty vibration sensor readings, and the final column identifies whether there was a fault with the machine at the time of the readings. In this column, 0 means there was no fault with the machine and 1 means a fault was identified.</p>
When creating tables for this work, you must name them as follows:<br/>

<b>FaultDataset</b> 
The uploaded datasets, must exist (and be named) in the following locations:<br/>

/FileStore/tables/FaultDataset.csv<br/>
This is to ensure that we can run your notebooks when testing your code (marks are allocated for your code running).
Your task as a data scientist is to do the following:<br/>

Load the dataset into a Spark DataFrame. You may want to consider carrying out some initial exploratory analysis of the data, which you are welcome to do using DataFrames, Spark SQL, Databricks visualisations, another visualisation library etc.
<br/>
Use MLlib to train a Decision Tree classification model (DecisionTreeClassifier algorithm) on the provided data and evaluate its performance. You will need to carry out all pre-processing steps, such as splitting the data into training and test sets.
<br/>
Track your experiment with MLflow. You must include screenshots from the Databricks Experiment UI in your report to evidence that you have done this.<br/>
<b>2.2.2. Extra features to be implemented for extra marks</b>
For this task you will get more marks (more than 69%) If you implement several extra features like follows (but are not limited to):<br/>

You can receive more marks for multiple runs as part of your experiment, for example, training models with different hyperparameters.<br/>

You can get more marks for testing different classification algorithms and comparing the results. However, you should note that you are not being marked on your understanding of the different machine learning algorithms, as this is not an assessed part of this module (this is covered in Machine Learning & Data Mining module).<br/>
