package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

//同样的也是按照Key将两个RDD中进行汇总操作
//不过会对每个Key所对应的两个RDD中的数据进行笛卡尔积计算
public class join {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("join");
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
        JavaPairRDD<String, Tuple2<String, String>> rdd3 = rdd.join(rdd1);
        System.out.println("join后："+rdd3.collect());
    }
}
