package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

//与groupByKey类似，却有不同。如(a,1), (a,2), (b,1), (b,2)。
// groupByKey产生中间结果为( (a,1), (a,2) ), ( (b,1), (b,2) )。而reduceByKey为(a,3), (b,3)。
public class reduceByKey implements Serializable {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Tuple2<String,Integer>> list= Arrays.asList(
            new Tuple2<>("A1",1),
            new Tuple2<>("A2",2),
            new Tuple2<>("A2",3),
            new Tuple2<>("A3",4),
            new Tuple2<>("A4",5),
            new Tuple2<>("A4",6)
            );
        JavaPairRDD<String, Integer> rdd=javaSparkContext.parallelizePairs(list);
        JavaPairRDD<String, Integer> rdd1= rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        System.out.println("reduceByKey使用之后："+rdd1.collect());



    }
}
