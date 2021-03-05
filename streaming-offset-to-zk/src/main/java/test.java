import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.util.*;

public class test {
    public static void main(String[] args) {
        int port = 9092;
        String topic = "2017-11-6-test";
        int time = -1;
        GetOffsetShellWrap offsetSearch = new GetOffsetShellWrap(topic, port, "192.168.72.88", time);
        Map<String, String> map = offsetSearch.getEveryPartitionMaxOffset();
        for (String key : map.keySet()) {
            System.out.println(key + "---" + map.get(key));
        }

        SparkConf conf = new SparkConf();
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        HashMap<String, String> params = new HashMap<>();
        Set<String> topics = new HashSet<>();
//        KafkaUtils.createDirectStream(jsc, StringSerializer.class,StringSerializer.class,StringDeserializer.class,StringDeserializer.class,params,Set<String>);
//        KafkaUtils.createDirectStream[String,String,StringDeserializer,StringDeserializer]()

        JavaPairInputDStream<String, String> input = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                params,
                topics);

        input.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String , String>>() {
            @Override
            public JavaPairRDD<String , String> call(JavaPairRDD<String, String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();



                return rdd;
            }
        });


        JavaInputDStream<String> directStream = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                params,
                new HashMap<TopicAndPartition, Long>(),
                new Function<MessageAndMetadata<String, String>, String>() {
                    @Override
                    public String call(MessageAndMetadata<String, String> stringStringMessageAndMetadata) throws Exception {

                        return null;
                    }
                }
        );

    }

}

