package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

//只能用在(K,V)类型，用来统计每个key的数据有多少个，返回一个(K,Int)。
public class countByKey {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("countByKey");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer,String>> list= Arrays.asList(
                new Tuple2<>(1,"窝窝头"),
                new Tuple2<>(1,"大饼"),
                new Tuple2<>(1,"馒头"),
                new Tuple2<>(2,"猪"),
                new Tuple2<>(2,"狗"),
                new Tuple2<>(2,"猫"),
                new Tuple2<>(3,"汽车")
        );
        JavaPairRDD rdd=javaSparkContext.parallelizePairs(list);
        Map<String, Long> result=rdd.countByKey();
        System.out.println("使用countByKey之后："+result);

    }

}
