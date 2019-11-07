package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

//将RDD保存在文件系统上，Spark会调用元素的toString方法作为一行数据。
//如果文件已经存在，会抛出异常
public class saveAsTextFile {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("saveAsTextFile").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,10,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
       rdd.saveAsTextFile("F:/text/a.txt");
    }

}
