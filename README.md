# Please add your team members' names here. 

## Team members' names 

1. Student Name: Alex Chavez

   Student UT EID: ajc6226

2. Student Name: Nick Pannell

   Student UT EID: nap2598

3. Student Name: Jerry Ehimuh

   Student UT EID: jee2352

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
   To run the code:
   Use "mvn clean package" to get a .jar file named "Gradient-Descent-0.1-SNAPSHOT-jar-with-dependencies.jar". Place the dataset in the same directory as the pom file. Run the code with "java -jar target/Gradient-Descent-0.1-SNAPSHOT-jar-with-dependencies.jar <input file name> <output dir name>".

   Output for the tasks will be in directories starting with "SLR" (simple linear regression), "GRD" (gradient descent), and "MGD" (multiple gradient descent).


# Add your Project REPORT HERE 


# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  output``` 

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
