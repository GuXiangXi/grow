package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
//相当于是先进行sample然后进行take操作
//返回一个数组，其中包含数据集的随机num元素样本，可以替换，也可以不替换，可以预先指定随机数生成器种子
public class takeSample {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("takeSample").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,3,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
        System.out.println("展示takeSample："+rdd.takeSample(false,2));
    }
}
