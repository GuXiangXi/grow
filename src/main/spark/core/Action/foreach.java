package Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class foreach implements Serializable {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("foreach").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,3,4,5,6,7);
        JavaRDD rdd=javaSparkContext.parallelize(list);
        rdd.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println("遍历rdd的数据："+o);
            }
        });

    }
}
