package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;
import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//将RDD中的partition进行减少，尤其是在上述的filter之后使用效果更好，
// 因为filter会可能会过滤掉大量的数据从而导致一个partition中的数据量很少，
// 这时候使用coalesce算子可以尽量的合并partition，一定程度少减少数据倾斜的问题
public class coalesce implements Serializable {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("coalesce").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> list = Arrays.asList("A1", "A2", "A3", "A4", "A5", "A5");
        JavaRDD<String> rdd = javaSparkContext.parallelize(list, 4);// 设置为四个partition
        //
        System.out.println("原数据分区数："+rdd.partitions().size());
        JavaRDD<String> coalesce = rdd.coalesce(2);
        System.out.println("原数据分区数："+coalesce.partitions().size());
    }
}
