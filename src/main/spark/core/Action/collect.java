package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;

//展示rdd数据的
//将集群中的其他节点（如果有的话）的数据pull到driver所在的机器上，
// 如果数据量过大的话可能会造成内存溢出的现象，所以官方的建议就是返回的数据量小的话会很有用
public class collect {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("collect").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,3,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
        System.out.println("展示collect："+rdd.collect());
    }
}
