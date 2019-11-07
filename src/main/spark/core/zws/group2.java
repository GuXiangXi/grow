package zws;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class group2 implements Serializable {
    @Test
    public void text() {
        SparkConf conf = new SparkConf().setAppName("wc11").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("C:\\Users\\gxx\\Desktop\\a.txt");
        int co = (int) input.count();
        long place = co / 2;
        JavaPairRDD<Double, Double> pairs;
        JavaPairRDD<Double, Iterable<Double>> group;
        int copies=100;//桶的个数
        //进入循环操作
        for (; co > 1000; ) {
            double max = Double.parseDouble(input.max(new t()));//最大值
            double min = Double.parseDouble(input.max(new t2()));//最小值
            double avg = (max - min) / copies;//桶之间的间隔
            //将javardd转换为有K，V的javaPairRDD
            pairs = input.mapToPair(line -> {
                Double b = Double.parseDouble(line);
                double c = 0;
                for (Double i = min; i <= max; i = i + avg) {
                    if (b >= i && b < i + avg) {
                        c = i;
                    }
                }
                return new Tuple2<>(c, Double.parseDouble(line)); });
            //reusult用于存放一个k拥有多少个v。
            Map<Double, Long> reusult = pairs.countByKey();//countByKey返回的是一个map
            Set set = reusult.keySet();//用set集合得到k的集合
            Object[] arr = set.toArray();
            Arrays.sort(arr);
            long p1 = 0;//用于判断是否已经到达中位数那个区
            Double p2 = 0.0;//记录中位数在那个区
            long p4 = 0;//用于接收中位数区，之前的数据个数
            for (Object key : arr) {
                p4 = p1;
                p1 = p1 + reusult.get(key);
                if (p1 >= place) {
                    p2 = (Double) key;
                    break;
                }
            }
            Double p3 = p2;//用于接收p2的值然后在 filter中使用
            System.out.println("p3:" + p3);
            group = pairs.groupByKey();
            group = group.sortByKey();
            group = group.filter(v1->{ if (v1._1.compareTo(p3) == 0 ? true : false) { return true; }return false;});
            place = place - p4;
        List<String> doubleList = new ArrayList<>();
        List<Iterable<Double>> list = group.lookup(p3);
        list.forEach(iterable -> {
            iterable.forEach(item -> {
                doubleList.add(item.toString());
            });
        });
        //doubleList.forEach(i->System.out.println(i));
        input=sc.parallelize(doubleList);
            co=(int)input.count();
        }
        /////////////////////////循环结束
        input=input.sortBy(x -> x, false, 10);
        System.out.println("input:"+input.count());
        System.out.println("inputcollect:"+input.collect());
        System.out.println("place:"+place);
        List<String> line = input.collect();
        double[] arr=new double[1000];
        int n=0;
        for (String val : line) {
            double v = Double.parseDouble(val);
            arr[n]=v;
            n++;
        }
        System.out.println("n:"+n);
        System.out.println("中位数为："+(arr[co-(int) place]+arr[co-(int) place-1])/2.0);
    }
}