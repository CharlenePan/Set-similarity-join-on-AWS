package comp9313.proj3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import java.lang.Math.ceil

object SetSimJoin{
  def main(args: Array[String]) {
  		val inputFile1 = args(0)
  		val inputFile2 = args(1)
  		val outputFolder = args(2)
  		val th = args(3).toDouble
  		val conf = new
  		SparkConf().setAppName("SetSimJoin").setMaster("local")
  		val sc = new SparkContext(conf)
  		val file1 = sc.textFile(inputFile1)
  		val file2 = sc.textFile(inputFile2)
  		
  		//---------------Stage 1 sort tokens by frequency----------------------
  		
  		//get the records in the files (id,elements)
  		val records1 = file1.map(line=>line.split(" ").map(_.toInt)).map(line=>(line(0),line.drop(1)))
  		val records2 = file2.map(line=>line.split(" ").map(_.toInt)).map(line=>(line(0),line.drop(1)))
  		
  		//get the tokens and calculate their frequency (element,frequency)
  		val tokens = sc.broadcast((records1.union(records2)).flatMap(_._2).map(x=>(x,1)).reduceByKey(_+_).collect.toMap)
  	  
  		//sort the elements of the records by frequency
  	  val sorted1 = records1.mapValues(x=>x.sortWith((x,y)=>x.toInt<y.toInt).sortBy(e=>tokens.value(e)))
  	  val sorted2 = records2.mapValues(x=>x.sortWith((x,y)=>x.toInt<y.toInt).sortBy(e=>tokens.value(e)))
  	  
  	  
  	  //---------------Stage 2 ----------------------
  	  //partition using prefixes
  	  //generate (prefix,(Rid,elements)) for file 1 
  	  val prefix1 = sorted1.flatMap{record=>
  	    val prefixlen = (math.ceil(record._2.length*(1-th))).toInt+1
  	    val prefix = record._2.take(prefixlen)
  	    for(i<-0 until prefix.length) yield (prefix(i),record)
  		}
  		
  		////generate (prefix,(Rid,elements)) for file 2 
  		val prefix2 = sorted2.flatMap{record=>
  	    val prefixlen_ = (math.ceil(record._2.length*(1-th))).toInt+1
  	    val prefix_ = record._2.take(prefixlen_)
  	    for(j<-0 until prefix_.length) yield (prefix_(j),record)
  		}
  		
  		//Verify similary
  		//R-S join (prefix,(records of R,records of S))
  		val union = prefix1.join(prefix2)
      val res = union.map{x=>
        val Rid = x._2._1._1
        val Sid = x._2._2._1
        val recordR = x._2._1._2
        val recordS = x._2._2._2
        val i = (recordR.toSet & recordS.toSet).size.toDouble
        val u = (recordR.toSet.size + recordS.toSet.size).toDouble-i
        val sim = (i/u)
        ((Rid,Sid),BigDecimal(sim).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble)
      }.filter{case(x,y)=>y>=th}.distinct
  		
      //sort the results and write to outputFolder
      val sortedres = res.map(x=>(x._1._1,x._1._2,x._2)).sortBy(x=>(x._1,x._2))
  		val final_res = sortedres.map(x=>"("+x._1+","+x._2+")"+"\t"+x._3)
  		final_res.saveAsTextFile(outputFolder)
  		
  }//end main
}
