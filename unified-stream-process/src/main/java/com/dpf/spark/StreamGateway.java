package com.dpf.spark;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import java.util.*;

public class StreamGateway {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        String appName="spark-streaming-task";
        sparkConf.setAppName(appName);
        sparkConf.setMaster("local[*]");
        // 设置序列化 方式
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 启用反压机制
        sparkConf.set("spark.streaming.backpressure.enabled","true");
        //最小摄入条数控制
        sparkConf.set("spark.streaming.backpressure.pid.minRate","1");
        //最大摄入条数控制
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition","100");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "online");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("source_offer");


        // 从kafka 获取数据
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream =
            KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        kafkaStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.stop();
    }
}
