/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.streaming

import kafka.serializer.StringDecoder
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Consumes messages from one or more topics in Kafka and writes to filesystem using the new Kafka OffsetManagement
  * Usage: KafkaDirectWithOffset <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  *
  */
object KafkaDirectWithOffset {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: KafkaDirectWithOffset <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args
    var totalCount = 0L
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    sparkConf.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](("bootstrap.servers" -> brokers), ("enable.auto.commit" -> "false"),
      ("value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"),
      ("key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"),
      ("group.id" -> "testGroupID"))
    //ssc, PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics, kafkaParams, fromOffsets)
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    var offsetRanges = Array[OffsetRange]()

   val lines = messages.transform{
     rdd =>
       offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
       rdd
   }.map(record => (record.key, record.value))


   lines.foreachRDD(rdd=>{

      if(!rdd.isEmpty()) {

        totalCount = totalCount + rdd.count()
        System.out.println("TotalCount " + totalCount + " Not Empty " + rdd.toString())
        rdd.saveAsTextFile("/tmp/lines" + System.currentTimeMillis()) //Keep from overwrting same file each time, add unique Time stamp

      }
    })
    messages.transform{
      rdd =>
        rdd.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        rdd
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
