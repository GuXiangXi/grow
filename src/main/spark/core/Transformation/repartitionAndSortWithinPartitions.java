package Transformation;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

//该方法根据partitioner对RDD进行分区，并且在每个结果分区中按key进行排序。
public class repartitionAndSortWithinPartitions implements Serializable {
    @Test
    public void text() {
        SparkConf sparkConf = new SparkConf().setAppName("repartitionAndSortWithinPartitions").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer,String>> list= Arrays.asList(
                new Tuple2<>(1,"A1"),
                new Tuple2<>(7,"A2"),
                new Tuple2<>(3,"A3"),
                new Tuple2<>(4,"A4"),
                new Tuple2<>(2,"A5"),
                new Tuple2<>(5,"A6"),
                new Tuple2<>(6,"A7")
        );
        JavaPairRDD<Integer,String> rdd=javaSparkContext.parallelizePairs(list);
        rdd=rdd.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                //总共4个分区将%2==0的k，v放到0分区，将%3==0的k，v放到3分区,剩下的放到2分区
                if((int)key%2==0){
                return 0;
                }else if((int)key%3==0){
                return 3;
                }
                else {
                    return 2;
                }
            }
            @Override
            public int numPartitions() {
                return 4;
            }
        });

        System.out.println(rdd.partitions().size());

        System.out.println("repartitionAndSortWithinPartitions之后结果："+rdd.collect());

    }
}
