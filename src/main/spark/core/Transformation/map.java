package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

public class map {
    @Test
    public void text(){
        SparkConf conf = new SparkConf().setAppName("wc11").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("C:\\Users\\gxx\\Desktop\\b.txt");
        JavaRDD<Integer> rdd1;
        System.out.println("使用map前"+rdd.collect());
        //通过对这个RDD的所有元素应用一个函数来返回一个新的RDD。
        rdd1=rdd.map(v1->{
            int v2;
            v2= Integer.parseInt(v1)+1;
            return v2;
         });
        System.out.println("使用map后"+rdd1.collect());
    }
}
