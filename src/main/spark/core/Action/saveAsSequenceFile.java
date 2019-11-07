package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

//将dataSet中元素以Hadoop SequenceFile的形式写入本地文件系统或者HDFS等。（对pairRDD操作）
public class saveAsSequenceFile {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("saveAsSequenceFile").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,10,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
       // rdd.saveAsSequenceFile("F:/text/a.txt");
    }

}
