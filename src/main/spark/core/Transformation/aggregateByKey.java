package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
//类似groupByKey
public class aggregateByKey implements Serializable {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("aggregateByKey").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> list= Arrays.asList("you,jump", "i,jump");
        JavaRDD<String> rdd=javaSparkContext.parallelize(list);
        JavaRDD<String> rdd1;


        rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(",")).iterator();
            }
        })
                //第一个参数输入的数据类型，第二个参数输出的Key的类型，第三个参数输出的Value的类型
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                })

        //第一个参数是初始值，如果是10就是每一个单词一开始就是10个，0就是按照0往上加，
        //第二个是局部进行计算，
        //第三个是全局计算，这个特点就是控制的比较细，使用比较复杂，还可以对字符串进行拼接
        .aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;//局部
             }
            },new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;//全局
                }
            }
        );
        System.out.println("reduceByKey使用之后："+rdd.collect());
    }
}
