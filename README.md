# Yelp data processing using SPARK on Mesos

## Prerequisite
Docker should be installed before using this image

## Apache Spark & Apache Mesos image

Built on:

1. Ubuntu: 16.04
2. Default JDK: openjdk-8
3. Mesos: Mesosphere Mesos 1.1.0
4. Apache Spark: 2.1.0

# Instructions

1. Download or Clone the repository using the below git command to a directory

      git clone https://github.com/anuraagprasaad/yelp-smack.git

2. Download the Yelp Challenge Data set(.tar or .tgz) to this cloned directory

3. If the name of this Yelp dataset is different from yelp_dataset_challenge_round9.tgz, then update the name in the ADD command inside the Docker file as below
  ADD <NEW_YELP_DATA_SET_NAME.tgz> /opt/yelp/

4. Ensure all the files and tar data files are in the above cloned directory

5. Run the below Docker build command

      sudo docker build -t newyorker/yelp .

6. Run the below Docker run command

      sudo docker run -ti --net=host newyorker/yelp-new spark-shell -i /opt/yelp/SparkSQLYelpDataProcessing.txt --driver-memory 10g --      executor-memory 10g --conf spark.mesos.executor.docker

-- You can change the memory as per your available system memory 
