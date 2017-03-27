package org.packtpub;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.functions;

import scala.Tuple2;
import scala.collection.Iterable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class RDDConversion 
{

 public static void main( String[] args )
    {
    	RDDConversion app = new RDDConversion();
    	System.setProperty("hadoop.home.dir", "C:/spark/spark-2.0.0/");
    	String sparkWarehouseDir = "/home/spark/spark-warehouse";
    	String fileName = args[0];
    	
    	SparkConf conf = new SparkConf().setAppName("RDDConversion").setMaster("local[*]");
    	JavaSparkContext sc = new JavaSparkContext(conf);
   
    	SparkSession mySparkSession = SparkSession.builder()
				.master("local")
				.appName("Java Spark-SQL Hive Integration ")
				.enableHiveSupport()
				.config("spark.sql.warehouse.dir", sparkWarehouseDir)
				.getOrCreate();

    	JavaRDD<String> dataFile = sc.textFile(fileName);
    	
    	JavaRDD<CallDetailRecord> cdr = dataFile.map(new Function<String,CallDetailRecord>(){
    		public CallDetailRecord call(String line) throws Exception{
    			String[] parts = line.split(",");
    			CallDetailRecord cdr = new CallDetailRecord();
    			cdr.setOriginNumber(parts[0]);
    			cdr.setTermNumber(parts[1]);
    			cdr.setOrigin(parts[2]);
    			cdr.setTermDest(parts[3]);
    			cdr.setDateTime(parts[4]);
    			cdr.setCallCharges(Long.parseLong(parts[5]));
    			return cdr;
    		}
    	});
    	
    	
    	Dataset<Row> cdrDataFrame = mySparkSession.createDataFrame(cdr, CallDetailRecord.class);
    	cdrDataFrame.show();
    	
       	
    }
    
    
    
}

