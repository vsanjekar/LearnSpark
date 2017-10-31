# LearnSparkJava

To get the data for wordcount example:

wget http://www.gutenberg.org/files/4300/4300.zip

unzip 4300.zip

mvn package

spark-submit --class word_processor.WordProcessor target/vsanjekar-SparkJavaTest-1.0-SNAPSHOT.jar 4300.txt
