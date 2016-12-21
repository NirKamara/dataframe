package dataFrame;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;


/**
 * Created by student12 on 21/12/2016.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("DF");
        JavaSparkContext sc  = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame ldDF = sqlContext.read().json("data/linkedin/*.json");

        // Q1
        ldDF.show();

        // Q2
        ldDF.printSchema();

        // Q3
        Tuple2<String, String>[] dtypes = ldDF.dtypes();
        for (Tuple2<String, String> dtype : dtypes) {
            System.out.println(dtype.toString());

        }


        // Q4




    }
}
