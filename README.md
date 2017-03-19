# Yelp data processing using Apache Spark & Apache Mesos image

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

      sudo docker build -t newyorker-yelp .

6. Run the below Docker run command

  sudo docker run -ti --net=host newyorker-yelp spark-shell -i /opt/yelp/YelpDataParser.scala --driver-memory 30g -- executor-memory 30g

## Notes:

-- You can change the driver-memory and executor-memory as per your available system memory 

-- That's it. All the queries will be automatically executed inside the Spark Shell.

-- This can easily be extended to execute an application JAR instead of a simple scala class. We can use Maven inside Dockerfile to build the application Jar or use COPY command inside the Docker file to copy the application Jar to /opt/Yelp/ and execute the Docker run command with spark-submit --master mesos://MESOS-MASTER:PORT --class FULLY-CLASSIFIED-CLASSFILE /opt/Yelp/JARFILE

-- There are few queries for each of the below Yelp Json files to ensure we can query these data sets.

      1. yelp_academic_dataset_business.json

      2. yelp_academic_dataset_user.json
      
      3. yelp_yelp_academic_dataset_checkin.json
      
      4. yelp_academic_dataset_tip.json
      
      5. Some Join operations
       
