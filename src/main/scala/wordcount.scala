import org.apache.spark._

object wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wc")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("C:/Users/gxx/Desktop/big.txt");
//    val count=file.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
//  count.foreach(o=>{
//    println(o)
//  })

    val count=file.flatMap(line => line.split(" "));
    println(count.count());
    //println(sc)
  }
}
