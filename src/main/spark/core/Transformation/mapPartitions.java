package Transformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.*;
import org.junit.Test;
import java.io.Serializable;


public class mapPartitions implements Serializable {
    @Test
    public void text(){
        SparkConf sparkConf = new SparkConf().setAppName("mapPartitions").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> list = Arrays.asList("A1","A2","A3","A4");
        JavaRDD<String> rdd=javaSparkContext.parallelize(list,3);
        final Map<String,Integer> scoreMap = new HashMap<>();
        scoreMap.put("A1",1);
        scoreMap.put("A2",2);
        scoreMap.put("A3",3);
        scoreMap.put("A4",4);


        System.out.println("使用mapPartition前："+rdd.collect());
        //通过对这个RDD的每个分区应用一个函数来返回一个新的RDD。
        JavaRDD<Integer> result = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<Integer> call(Iterator<String> iterator) throws Exception{
                List<Integer> list = new ArrayList<>();
                while(iterator.hasNext()){
                    String name = iterator.next();
                    int score = scoreMap.get(name);
                    list.add(score);
                }
                return list.iterator();
            }
        });
        System.out.println("使用mapPartition后："+result.collect());
    }
}
