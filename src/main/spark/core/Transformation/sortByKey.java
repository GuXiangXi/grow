package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
//根据Key进行排序操作，如果第一个参数为true，则结果为升序，反之为降序。第二个参数就是决定执行的task(任务)数目
public class sortByKey {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("sortByKey");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer,String>> list=Arrays.asList(
                new Tuple2<>(1,"A1"),
                new Tuple2<>(1,"A2"),
                new Tuple2<>(3,"A3"),
                new Tuple2<>(4,"A4"),
                new Tuple2<>(2,"A5"),
                new Tuple2<>(5,"A6"),
                new Tuple2<>(6,"A7")
        );
        JavaPairRDD<Integer,String> rdd=javaSparkContext.parallelizePairs(list);
        rdd=rdd.sortByKey(true,2);
        System.out.println("sortbyKey之后结果："+rdd.collect());
    }
}
