package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
//计数
//每次传入两个参数通过函数func得到一个返回值，然后使用该值继续与后面的数进行调用func，
// 直到所有的数据计算完成，最后返回一个计算结果。
public class reduce implements Serializable {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,3,5,8,9);
        JavaRDD<Integer> rdd=javaSparkContext.parallelize(list);
        int sum=rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
    System.out.println("reduce之后的结果："+sum);


    }
}
