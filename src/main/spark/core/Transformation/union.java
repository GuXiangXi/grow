package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
//合并两个rdd
public class union {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("union").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> list= Arrays.asList("A1","A2","A3","A4","A5","A6","A7","A8");
        List<String> list2= Arrays.asList("B1","B2","B3","B4","B5","B6","B7","A8");
        JavaRDD<String> rdd1=javaSparkContext.parallelize(list);
        JavaRDD<String> rdd2=javaSparkContext.parallelize(list2);

        JavaRDD<String>rdd3=rdd1.union(rdd2);
        System.out.println("union之后;"+rdd3.collect());

    }
}
