package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

//通过管道将元素创建的RDD返回到分叉的外部流程
public class pipe {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("pipe").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,2,5,3,5,1,7,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
        JavaRDD<Integer> repartition = rdd.repartition(5);

        JavaRDD<String> rdd1=rdd.pipe("head -n 1");
        System.out.println("使用pipe之后的结果："+rdd1.collect());




    }
}
