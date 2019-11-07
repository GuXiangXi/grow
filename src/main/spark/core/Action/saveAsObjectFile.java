package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

//将数据集中元素以ObjectFile形式写入本地文件系统或者HDFS等。
//如果文件已经存在，会抛出异常
public class saveAsObjectFile {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("saveAsObjectFile").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,10,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
        rdd.saveAsObjectFile("F:/text/e.txt");
    }


}
