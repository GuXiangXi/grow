package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
//返回一个新的RDD，首先将一个函数应用到这个RDD的所有元素上，然后将结果压扁
public class flatMap {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("flatMap");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> list = Arrays.asList("A1 1","A2 2","A3 3","A4 4");
        JavaRDD<String> listRdd = javaSparkContext.parallelize(list);

        System.out.println("使用flatMap前："+listRdd.collect());
        JavaRDD<String> result = listRdd.flatMap(s->{
                s=s+2;
                return Arrays.asList(s.split(" ")).iterator();
        });
        System.out.println("使用flatMap后："+result.collect());
    }
}
