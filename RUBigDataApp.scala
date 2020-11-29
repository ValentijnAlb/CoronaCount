package org.rubigdata

import org.apache.spark.sql.SparkSession
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.{URL, URI}
import java.io.File
import sys.process._

object RUBigDataApp {
  def main(args: Array[String]) {
    val uname = "s1005665"
    val filename = "nos.warc.gz"
    val warcfile = s"hdfs:/user/$uname/$filename"
    val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val url = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-40/segments/1600400209665.4/wet/*warc.wet.gz"
    val fs = FileSystem.get(new URI("hdfs:/"), conf)

    if(!fs.exists(new Path(s"hdfs:/user/$uname/$filename"))){
        (new URL(url) #> new File(filename)).!
        val localfile = new Path(filename)
        val remotefile = new Path(s"hdfs:/user/$uname/$filename")
        
        fs.copyFromLocalFile(false, true, localfile, remotefile)
    }

    val warcs = sc.newAPIHadoopFile(
                  warcfile,
                  classOf[WarcGzInputFormat],               // InputFormat
                  classOf[NullWritable],                    // Key
                  classOf[WarcWritable]                     // Value
                )
    val content = warcs.
                map{ _._2 }.
                filter{ _.isValid() }.
                map{ _.getRecord() }.
                filter{ record => record.getHeader().getUrl() != null }.
                map{ x => {
                            var b = ""
                            for(a <- 0 to x.getHeader().getContentLength().toInt){
                                b += x.getRecord().read().toChar
                            }
                            b
                          }
                }.cache()

    val words = content.flatMap{ x => x.replaceAll("""[\p{Punct}&&[^.]]""", "").replaceAll("\n"," ").trim.toLowerCase.split(" ") }
    val wordcount = words.map{ x => 1 }.
                          reduce(_ + _)
    val coronacount = words.filter{ word => word.equals("corona") }.
                            map{ x => 1 }.
                            reduce(_ + _)
    val coronafrequency = coronacount.toDouble/wordcount.toDouble

    println("\n########## OUTPUT ##########")
    println("The relative frequency of the word 'corona' on 'nos.nl' is %f".format(coronafrequency))
    println("########### END ############\n")
    spark.stop()
  }
}
