# run the following docker container for min-io
mkdir -p ~/minio/data
docker run -d \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   -v ~/minio/data:/data \
   -e "MINIO_ROOT_USER=vijay" \
   -e "MINIO_ROOT_PASSWORD=donthireddy" \
   quay.io/minio/minio server /data --console-address ":9001"

# for spark run the following code before the spark-submit
export SPARK_LOCAL_IP="127.0.0.1"

# package the jar
cd /Users/donthireddy/code/mygit/databricks-simple
mvn clean package

# submit the jar file to spark (make sure to use the jar-with-dependencies)
cd ~/code/spark-3.5.5-bin-hadoop3/
#./bin/spark-submit --class=com.niharsystems.Main /Users/donthireddy/code/mygit/databricks-simple/target/databricks-simple-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --jars /Users/donthireddy/code/mygit/databricks-simple/libs/delta-core_2.12-2.4.0.jar,/Users/donthireddy/code/mygit/databricks-simple/libs/delta-storage-2.4.0.jar,/Users/donthireddy/code/mygit/databricks-simple/libs/hadoop-aws-3.3.4.jar,/Users/donthireddy/code/mygit/databricks-simple/libs/aws-java-sdk-bundle-1.12.533.jar --class=com.niharsystems.Main /Users/donthireddy/code/mygit/databricks-simple/target/databricks-simple-1.0-SNAPSHOT-jar-with-dependencies.jar

