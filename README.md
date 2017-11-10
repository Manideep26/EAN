## Calling the file transfer spark job:
`spark-submit --master yarn-client --queue root.spark-submit --master yarn-client --queue root.ecpcitickerfeed --class com.thomsonreuters.ecp.spark.ParquetToHive ean-to-neptune-1.0-SNAPSHOT.jar --class com.thomsonreuters.ecp.cdap.actions.GetDataFromEAN ean-to-neptune-1.0-SNAPSHOT.jar`

## Calling BCPToParquet spark job

(Provide the 5 files generated by the previous step as arguments)

`spark-submit --master yarn-client --queue root.ecpcitickerfeed --jars spark-csv_2.10-1.2.0.jar,commons-csv-1.1.jar --class com.thomsonreuters.ecp.spark.BCPToParquet ean-to-neptune-1.0-SNAPSHOT.jar EANVALUEDOMAINENUMERATION_20150305_1848_sm2.bcp ...`


'spark-submit --master yarn-client --queue root.ecpcitickerfeed --jars spark-csv_2.10-1.2.0.jar,commons-csv-1.1.jar --driver-java-options="-Dfs.default.name=hdfs://Venus" --class com.thomsonreuters.ecp.spark.BCPToParquet ean-to-neptune-1.0-SNAPSHOT.jar EANFMMIDENTIFIER_20170721_0646.bcp  EANFMMSOURCENAME_20170721_0654.bcp  EANINSTRUMENT_20170821_1246.bcp  EANORGANIZATION_20170821_1246.bcp  EANQUOTE_20170821_1247.bcp'

## Making the Hive table

(Provide the Hive path from the previous step to the `hiveSourceFilename` System property.)

Default path:

`spark-submit --master yarn-client --queue root.ecpcitickerfeed --class com.thomsonreuters.ecp.spark.ParquetToHive ean-to-neptune-1.0-SNAPSHOT.jar`

Custom path:

`spark-submit --master yarn-client --queue root.ecpcitickerfeed --driver-java-options="-DhiveSourceFilename=$PARQUET_FILENAME" --class com.thomsonreuters.ecp.spark.ParquetToHive ean-to-neptune-1.0-SNAPSHOT.jar`

Different namespace too:

`spark-submit --master yarn-client --queue root.ecpcitickerfeed --driver-java-options="-DhiveSourceFilename=$PARQUET_FILENAME -DclusterNamespace=$MYNAMESPACE" --class com.thomsonreuters.ecp.spark.ParquetToHive ean-to-neptune-1.0-SNAPSHOT.jar`

## Creating and querying a table with parquet file:

`scala> val parqfile = sqlContext.read.parquet("testBCP2.parquet")`

`scala> parqfile.registerTempTable("eanTableTestTest")`

`scala> val alltherecords = sqlContext.sql ("Select * from eanTableTestTest")`

`scala> alltherecords.show()`

## Getting to a spark-shell:
`spark-shell --queue root.ecpcitickerfeed`

## Prepare the jar file:
`mvn clean package`
or run maven clean and maven compile targets in IntelliJ
## Set HDFS
--driver-java-options="-Dfs.default.name=hdfs://Venus"

