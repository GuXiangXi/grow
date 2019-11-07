package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
//进行重分区（增加减少都可以）
//用来增加parition，并且会将其中的数据进行平衡操作，使用shuffle操作。
public class repartition {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("repartition").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list= Arrays.asList(1,6,3,3,5,7,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list,4);
        System.out.println("repartition之前："+rdd.partitions().size());
        rdd=rdd.repartition(3);
        System.out.println("repartition之后："+rdd.partitions().size());
    }
}
