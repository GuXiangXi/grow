package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
//返回RDD中前n个元素，并按默认顺序排序（升序）或者按自定义比较器顺序排序。
public class takeOrdered {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("takeOrdered").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,10,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
        System.out.println("展示takeSample："+rdd.takeOrdered(3));
    }

}
