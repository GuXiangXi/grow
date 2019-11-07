package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//返回分组元素的RDD
public class groupByKey implements Serializable {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("groupByKey");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Tuple2<String,Integer>> list=Arrays.asList(
                new Tuple2<>("A1",1),
                new Tuple2<>("A2",2),
                new Tuple2<>("A2",3),
                new Tuple2<>("A4",4),
                new Tuple2<>("A6",5)
        );

        JavaPairRDD<String,Integer> rdd=javaSparkContext.parallelizePairs(list,2);
        JavaPairRDD<String,Iterable<Integer>> rdd1=rdd.groupByKey();
        System.out.println("使用groupBykey之后："+rdd1.collect());
        rdd1.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.err.println(stringIterableTuple2._1+":"+stringIterableTuple2._2);
            }
        });







    }
}
