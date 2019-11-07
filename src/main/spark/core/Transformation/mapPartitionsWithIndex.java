package Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.codehaus.janino.Java;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;

public class mapPartitionsWithIndex implements Serializable {
    @Test
    public void txet(){
        SparkConf sparkConf = new SparkConf().setAppName("mapPartitionWithindex").setMaster("local");
        JavaSparkContext javasparkContext = new JavaSparkContext(sparkConf);
        List<String> list = Arrays.asList("A1","A2","A3","A4");

        //将list转为RDD并且分为2个partition
        JavaRDD<String> rdd=javasparkContext.parallelize(list,2);
        System.out.println("使用mapPartitionWithindex之前："+rdd.collect());
        // Function2入参：第一个参数为partition的index,第二个为入参，第三个为返回值

        //与mapPartitions类似，但需要提供一个表示分区索引值的整型值作为参数，因此function必须是（int， Iterator<T>）=>Iterator<U>类型的
        JavaRDD<String> resultRDD = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> iterator) throws Exception {
                List<String> nameList = new ArrayList<>();
                while (iterator.hasNext()){
                    nameList.add(integer+":"+iterator.next());
                }
                return nameList.iterator();
            }
        },true);

        System.out.println("使用mapPartitionWithindex之后："+resultRDD.collect());

    }
}
