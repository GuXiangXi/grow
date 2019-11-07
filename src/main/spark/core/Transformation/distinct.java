package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

//返回包含源数据集的不同元素的新数据集。
public class distinct {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("distinct").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> list= Arrays.asList("A1","A2","A3","A2","A1");

        JavaRDD<String> rdd=javaSparkContext.parallelize(list);
        System.out.println("distinct之前的效果："+rdd.collect());
        rdd=rdd.distinct();
        System.out.println("distinct之后的效果："+rdd.collect());


    }
}
