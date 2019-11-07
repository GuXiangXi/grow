package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

public class filter {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd=javaSparkContext.textFile("C:\\Users\\gxx\\Desktop\\b.txt");
        //返回一个新的RDD，其中只包含满足条件的元素
        System.out.println("使用filter之前："+rdd.collect());
        rdd=rdd.filter(v1 -> {
            if ( Integer.parseInt(v1)>5) {
                return true;
            }
            return false;
        });
        System.out.println("使用filter之后："+rdd.collect());
    }
}
