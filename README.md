## COGNITIVE CLASS.ai: SPARK FUNDAMENTALS II - Labs Exercises Solved in PYTHON

This GIT is the Python solution to [Spark Fundamentals II](https://courses.competencies.ibm.com/courses/course-v1:BDU+BD0212EN+v1/about) course hands on laboratory exercises. The original exercises were programmed only in Scala and in order to develop my Python skills and get be used with Spark I solved all the execrcises using Pyhton 2.7.

### Requirements:

  * Python 2.7 installed
  * Spark 2.3.3  installed

### Directory Structure:

  * data.zip : data files used in the labs
  * Labs: Python files with the labs solutions
  * PDF: PDF files with labs exercise in Scala

### Lab Exercise 1:

The CLI command shall be executed in the Labs directory.

CLI to launch the exercise in the local machine: spark-submit --master local[2] lab1.py

If you are using LINUX ans just want to see only the lab1.py output use the grep command: 

CLI to launch the exercise in the local machine: spark-submit --master local[2] lab1.py | grep LAB1

### Lab Exercise 2:

The CLI command shall be executed in the Labs directory.

CLI to launch the exercise in the local machine: spark-submit --master local[2] lab2.py

If you are using LINUX ans just want to see only the lab1.py output use the grep command: 

CLI to launch the exercise in the local machine: spark-submit --master local[2] lab2.py | grep LAB2

### Lab Exercise 3:

#### Issue with pickles

It was necessary put my helper classes in other file because currently pyspark uses pickles to serialize the objects, and it don't support pickle an object in the current script ('main'). The workaround suggested by Davies Liu is to
put the classes into a separate module and import it on [Spark returning Pickle error: cannot lookup attribute](https://stackoverflow.com/questions/28569374/spark-returning-pickle-error-cannot-lookup-attribute). Other issue with pickle is that it do no support serialize Inner Classes, this is the reason why I did not put the
Trip and Station classes under an Utils classes. This was my first try until I have discovered the issue with pickle. (Juliana Oliveira - [Multiprocessing Serialization in Python with Pickle](https://medium.com/@jwnx/multiprocessing-serialization-in-python-with-pickle-9844f6fa1812))

In this labs was necessary create two helper classes nested under the class Utils. One class was used to store Trip objects and the other class was used to store teh Station object. In both classes the constructor parse the input files to its respective class attribute.

The CLI command shall be executed in the Labs directory.

CLI to launch the exercise in the local machine: spark-submit --master local[2] --py-files utils.py lab3.py

If you are using LINUX ans just want to see only the lab1.py output use the grep command: 

CLI to launch the exercise in the local machine: spark-submit --master local[2] --py-files utils.py lab3.py | grep LAB3

### Lab Exercise 4:


### Lab Exercise 5:
