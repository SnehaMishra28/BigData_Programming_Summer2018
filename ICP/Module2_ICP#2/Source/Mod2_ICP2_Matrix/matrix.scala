import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

object SparkMatrixMultiplication {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val matrix1data=sc.textFile("data/A")
    val matrix2data=sc.textFile(path = "data/B")

    val matrix1=matrix1data.zipWithIndex()
      .flatMap{
      case(line,i)=>{
        var j =0
        val linesplit=line.split(" ")
        linesplit.map(f=>{
          j = j+1
          ("A",i+1,j.toLong,f.toInt)
        })
    }}


    val matrix2=matrix2data.zipWithIndex()
      .flatMap{
        case(line,j)=>{
          var k =0
          val linesplit=line.split(" ")
          linesplit.map(f=>{
            k = k+1
            ("B",j+1,k.toLong,f.toInt)
          })
        }}

    val lastelement1=matrix1.top(1)
    val lastelement2=matrix2.top(1)

    if(lastelement1(0)._3==lastelement2(0)._2)
      {
        println("Matrix A with " + lastelement1(0)._2 +" X "+lastelement1(0)._3)
        println("Matrix B with " + lastelement2(0)._2 +" X "+lastelement2(0)._3)
        println("Are compatible for Multiplication")

        val ABi=sc.broadcast(lastelement1(0)._2)
        val ABk=sc.broadcast(lastelement2(0)._3)
        val matrix11=matrix1.flatMap(f=>{
          val kRange=Range(1,ABk.value.toInt+1)
          kRange.map(k=>
            //i,k
            ((f._2,k.toLong,f._3,f._1),(f._1,f._3,f._4))
          )
        })
        val matrix22=matrix2.flatMap(f=>{
          val iRange=Range(1,ABi.value.toInt+1)
          iRange.map(i=> {

            ((i.toLong, f._3,f._2,f._1), (f._1, f._2, f._4))
          })
        })

        val mm = matrix11++matrix22

        val mmr=mm.map(f=>{
          ((f._1._1,f._1._2,f._1._3),f._2)
        }).reduceByKey{case(a,b)=>{
          var mul= 0
          var st=""
            mul = a._3 * b._3
            st=a._1+b._1
            println("Inside")
          (st,a._2,mul)
        }}
        mmr.collect()
      }
    else
      {
        println("Matrix A with " + lastelement1(0)._1 +" X "+lastelement1(0)._2)
        println("Matrix B with " + lastelement2(0)._1 +" X "+lastelement2(0)._2)
        println("Are NOT compatible for Multiplication")
      }

    println("")
  }

}