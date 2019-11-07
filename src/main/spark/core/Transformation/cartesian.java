package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

//相当于进行了一次笛卡尔积的计算，将两个RDD中的数据一一对应起来
public class cartesian implements Serializable {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("cartesian");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<String> list= Arrays.asList("窝窝头","大饼","馒头");
        List<String> list1=Arrays.asList("好吃","真香");

        JavaRDD<String> rdd=javaSparkContext.parallelize(list);
        JavaRDD<String> rdd1=javaSparkContext.parallelize(list1);

        JavaPairRDD<String, String> rdd2=rdd.cartesian(rdd1);
        System.out.println("cartesian使用之后："+rdd2.collect());

        rdd.cartesian(rdd1).foreach(new VoidFunction<Tuple2<String,String>>() {

            @Override
            public void call(Tuple2<String, String> arg0) throws Exception {
                System.out.println(arg0);
            }
        });



    }
}
