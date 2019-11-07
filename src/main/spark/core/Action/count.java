package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
//计数
public class count {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("count").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,3,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
        System.out.println("展示count："+rdd.count());
    }
}
