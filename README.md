# raw-flink
# Installation guide for submitting job on flink cluster  


## Prerequisites

- python>=3.8,<3.12
- java 11.x
- flink version : 1.20
- flink jars : kafka-clients-3.4.0.jar
- Kafka cluster (with accessible brokers)
- Required Python packages (see `requirements.txt`)


git clone https://github.com/naveen-simpleenergy/raw-flink
cd your-repository 

Install the required dependencies:
```sh
pip install -r requirement.txt
```

## Configuration
Configure the service by setting the necessary environment variables in a .env file at the root of the project:
```sh
KAFKA_TOPIC=
KAFKA_BROKERS=
KAFKA_AUTO_OFFSET_RESET=
SECURITY_PROTOCOL=
SASL_MECHANISM=
SASL_USERNAME=
SASL_PASSWORD=
```

- Flink commands for installation 
 1. Install flink first then jars 
  ```
  wget https://downloads.apache.org/flink/flink-1.20.0/flink-1.20.0-bin-scala_2.12.tgz
  tar -xvzf flink-1.20.0-bin-scala_2.12.tgz
  mv flink-1.20.0 

  ```

 2. Set environment for the flink
    ```
    echo 'export FLINK_HOME=~/flink' >> ~/.bashrc
    echo 'export PATH=$FLINK_HOME/bin:$PATH' >> ~/.bashrc
    source ~/.bashrc 

    ```

 3. Install the jar files 
    ```
    curl -o $FLINK_HOME/lib/kafka-clients-3.4.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
    ```

# For running the script 
   ```
   1. Create a dependency.zip file by running command = ./zip.sh
   2. Start the cluster from flink directory => ./flink/bin/start-cluster.sh
   3. Submit the job using => flink run --pyExecutable  /path/bin/python  -pyfs "dependencies.zip"  -py main.py -d
   path = your virtual environment path like /home/naveen/raw-flink/.venv

   ```
