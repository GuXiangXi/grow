package zws;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

class t implements Serializable,Comparator{
    @Override
    public int compare(Object o1, Object o2) {
        double v1 = Double.parseDouble(o1.toString());
        double v2 = Double.parseDouble(o2.toString());
        return v1-v2>0?1:-1;
    }
}

class t2 implements Serializable,Comparator{
    @Override
    public int compare(Object o1, Object o2) {
        double v1 = Double.parseDouble(o1.toString());
        double v2 = Double.parseDouble(o2.toString());
        return -(v1-v2>0?1:-1);
    }
}

public class text6 implements Serializable {
    @Test
    public void text25()  {
        long t1 = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setAppName("wc11").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\gxx\\Desktop\\a.txt");
        JavaRDD<String> l2=lines;//将lines复制一份
        JavaRDD<String> l3=lines;
        JavaRDD<String> l4=lines;
        JavaRDD<Integer> lineLengths = l2.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println("加载数据耗时："+(System.currentTimeMillis()-t1)/1000+"秒");
        long t = System.currentTimeMillis();

        int place=(int) l2.count()/2;//记住是数据中的第几个,如果是双数就是place和place+1加起来除以2
        int co= (int) l2.count();//计数剩余数据
        int a=0;//统计循环次数
        System.out.println("place初始值为："+place);
        int bz=0;//用于统计已经砍掉的数据中的一部分
        int lz=0;

        for(;co>1000;) {
            double max = 0;//最大值
            double min = 10;//最小值
            max=Double.parseDouble (l2.max(new t()));
            min=Double.parseDouble (l2.max(new t2()));

            double sum =(max + min)/ 2;//选取一个最大最小值的中间的值作为比对
            int b = 0;//大于sum的个数；
            int l = 0;//小于等于sum的个数
            l3 = l3.filter(st -> {
                double d = Double.parseDouble(st);
                if (d < sum) { return true; }return false;
            });
            b=(int)l3.count();
            l4 = l4.filter(st -> {
                double d = Double.parseDouble(st);
                if (d > sum) { return true; }return false;
            });
            l=(int)l4.count();

            System.out.println("place:"+place);

                    if (b > l) {
                        lz=lz+l;
                        if(lz>=(int)lines.count()/2){
                            l2 = l2.filter(st -> { double d = Double.parseDouble(st);if (d > sum) { return true; }return false; });
                            l3=l2;
                            l4=l2;

                        }else {
                        l2 = l2.filter(st -> { double d = Double.parseDouble(st);if (d < sum) { return true; }return false; });
                        l3=l2;l4=l2;
                        if (place> l){place=place- l;}
                        }

                    }

                    else if(l ==place){System.out.println("中位数找到了");break;}
                    else if (b < l){
                        bz=bz+b;
                        if (bz>=(int)lines.count()/2){
                            l2 = l2.filter(st -> { double d = Double.parseDouble(st);if (d < sum) { return true; }return false; });
                            l3=l2;l4=l2;
                            if (place> l){place=place- l;}
                        }
                        else {
                        l2 = l2.filter(st -> { double d = Double.parseDouble(st);if (d > sum) { return true; }return false; });
                        l3=l2;l4=l2;
                        }
                    }
                    else if (place == 500000000) { System.out.println("中位数为：" + sum); break;}

            co=(int)l2.count();
            a++;
        }

        System.out.println(place);
        List<String> line = l2.collect();
        double[] arr=new double[1000];
        int n=0;
        for (String val : line) {
            double v = Double.parseDouble(val);
            arr[n]=v;
            //System.out.println(arr[n]);
            n++;
        }
        System.out.println("n:"+n);

        for (int i=0;i<arr.length;i++){
            for(int j=0;j<arr.length-1-i;j++){
                if(arr[j]<arr[j+1]){
                    double temp=arr[j];
                    arr[j]=arr[j+1];
                    arr[j+1]=temp;
                }
            }
        }

        System.out.println("中位数为："+(arr[place]+arr[place-1])/2.0);
        System.out.println("循环了："+a+"次");
        System.out.println("当前中位数位置："+place);
        System.out.println("当前数据长度："+l2.count());
        System.out.println("计算耗时："+(System.currentTimeMillis()-t)/1000+"秒");
        System.out.println("一共有："+lineLengths.count()+"行数据");


    }
}
