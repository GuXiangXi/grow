package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
//就是将RDD中的前多少数据返回过来，返回结果为数组形式。
public class take {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("take").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,3,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
        System.out.println("展示takeSample："+rdd.take(3));
    }
}
