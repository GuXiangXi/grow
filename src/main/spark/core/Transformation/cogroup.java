package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
//将两个RDD按照Key进行汇总，第一个RDD中的Key对应的数据放在一个Iterable中，
// 第二个RDD中同样的Key对应的数据放在一个Iterable中，
// 最后得到一个Key，对应两个Iterable的数据。第二个参数就是指定task数量。
public class cogroup {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("cogroup").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Tuple2<String,String>> list= Arrays.asList(
                new Tuple2<>("A1","语文"),
                new Tuple2<>("A3","数学"),
                new Tuple2<>("A3","英语"),
                new Tuple2<>("A2","语文"),
                new Tuple2<>("A4","数学"),
                new Tuple2<>("A3","语文")
        );
        List<Tuple2<String,String>> list1=Arrays.asList(
                new Tuple2<>("A1","1"),
                new Tuple2<>("A2","2"),
                new Tuple2<>("A2","3"),
                new Tuple2<>("A3","4"),
                new Tuple2<>("A4","5"),
                new Tuple2<>("A4","6")
        );

        JavaPairRDD<String,String> rdd=javaSparkContext.parallelizePairs(list);
        JavaPairRDD<String,String> rdd1=javaSparkContext.parallelizePairs(list1);
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> rdd3 = rdd.cogroup(rdd1);
        System.out.println("cogroup使用之后："+rdd3.collect());

    }
}
