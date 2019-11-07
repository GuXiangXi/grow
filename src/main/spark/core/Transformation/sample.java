package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//随机采样
//使用给定的随机数生成器种子对数据的一部分进行采样，不论是否替换。返回这个RDD的一个采样子集
public class sample implements Serializable {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("sample");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> list= Arrays.asList("A1","A2","A3","A4","A5","A6","A7","A8");
        JavaRDD<String> rdd=javaSparkContext.parallelize(list);
        System.out.println("sample使用前："+rdd.collect());
        // 第一个参数决定取样结果是否可重复,第二个参数决定取多少比例的数据,第三个是自定义的随机数种子，如果传入一个常数则每次产生的值一样
        rdd.sample(false, 0.5).foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(String arg0) throws Exception {
                System.out.println("sample使用后："+arg0);
            }
        });

    }
}
