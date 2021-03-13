import kafka.api.OffsetRequest;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.mortbay.log.Log;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class HdfsUtils {
    public static void main(String[] args) throws IOException, InterruptedException {
//        Configuration conf = new Configuration();
////        conf.set("fs.defaultFS","hdfs://192.168.72.161:8020");
////        FileSystem fs = FileSystem.get(conf);
////        boolean exists = fs.exists(new Path("/spark/streaming/stop"));
////        System.out.println(exists);
////        fs.close();

        SparkConf conf = new SparkConf().setMaster("local[1]");
        conf.setAppName("java streaming")
                .set("spark.streaming.stopGracefullyOnShutdown","true")
                .set("spark.streaming.backpressure.enable","true")
                .set("spark.streaming.backpressure.initialRate","5000")
                .set("spark.streaming.kafka.maxRatePerPartition","2000");
        HashMap<String, String> params = new HashMap<>();
        params.put("bootstrap.servers","192.168.72.161:9092,192.168.72.162:9092,192.168.72.163:9092");
        params.put("auto.offset.reset", OffsetRequest.LargestTimeString());
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        HashSet<String> topics = new HashSet<>();
        String topic = "dc_test";
        topics.add(topic);
        ZkClient zkClient = new ZkClient(
                "192.168.72.161:2181,192.168.72.162:2181,192.168.72.163:2181",
                3000,
                3000,
                ZKStringSerializer$.MODULE$);
        String zkPath = "/sparkStreaming/20171128";
        final OffsetManager offsetManager = new OffsetManager();
        Map<TopicAndPartition, Long> readOffsets = offsetManager.readOffsets(zkClient, zkPath, topic);

        JavaPairInputDStream<String, String> input = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                params,
                topics);
        input.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, String>> datas) throws Exception {
                        while (datas.hasNext()){
                            Tuple2<String, String> data = datas.next();
                            System.out.println("data:======="+data);
                        }
                    }
                });

                OffsetRange[] offsetRanges = ((HasOffsetRanges) (rdd.rdd())).offsetRanges();
                System.out.println(offsetRanges.length);

            }
        });

//        input.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> rdd) throws Exception {
//
//                OffsetRange[] offsetRanges = ((HasOffsetRanges) (rdd.rdd())).offsetRanges();
//                System.out.println(offsetRanges.length);
//                List<String> collect = rdd.collect();
//                for (String s : collect) {
//
//                System.out.println("data:   ==========================="+s);
//                }
//            }
//        });


//        JavaInputDStream<String> result = KafkaUtils.createDirectStream(
//                jsc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                String.class,
//                params,
//                readOffsets,
//                new Function<MessageAndMetadata<String, String>, String>() {
//                    @Override
//                    public String call(MessageAndMetadata<String, String> s) throws Exception {
//                        return s.message();
//                    }
//                }
//        );
//
//
//        result.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> rdd) throws Exception {
//                OffsetRange[] offsetRanges = ((HasOffsetRanges) (rdd.rdd())).offsetRanges();
//                System.out.println(offsetRanges.length);
//
//                List<String> list = rdd.collect();
//                for (String s : list) {
//                    System.out.println(s);
//                }
//            }
//        });


        jsc.start();

        jsc.awaitTermination();
    }
}
