import kafka.api.OffsetRequest;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class sds implements Serializable {

    private static final Logger log = LogManager.getLogger("javaSparkStreaming");
    private final static ZkClient zkClient = new ZkClient(
            "192.168.72.161:2181,192.168.72.162:2181,192.168.72.163:2181",
            3000,
            3000,
            ZKStringSerializer$.MODULE$);

    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext jsc = new sds().createStreamingContext(true, true);
        jsc.start();
        jsc.awaitTermination();
    }

    /**
     * -构建streamingContext
     *
     * @param isLocal         -是否是本地调试
     * @param firstReadLatest -是否从最新offset读取数据
     * @return -返回业务操作后的流对象
     */
    private JavaStreamingContext createStreamingContext(boolean isLocal, boolean firstReadLatest) {
        SparkConf conf = new SparkConf();
        conf.setAppName("java streaming")
                .set("spark.streaming.stopGracefullyOnShutdown", "true")
                .set("spark.streaming.backpressure.enable", "true")
                .set("spark.streaming.backpressure.initialRate", "5000")
                .set("spark.streaming.kafka.maxRatePerPartition", "2000");
        if (isLocal) conf.setMaster("local[1]");

        HashMap<String, String> params = new HashMap<>();
        params.put("bootstrap.servers", "192.168.72.161:9092,192.168.72.162:9092,192.168.72.163:9092");
        String readLatest = firstReadLatest ? OffsetRequest.LargestTimeString() : OffsetRequest.SmallestTimeString();
        params.put("auto.offset.reset", readLatest);


        final String zkPath = "/sparkStreaming/20171128";
        String topic = "dc_test";

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        manager manager = new manager();
        Map<TopicAndPartition, Long> readOffsets = manager.readOffsets(zkClient, zkPath, topic);
        if (readOffsets == null) {
            JavaPairInputDStream<String, String> largestKafkaStream = createLargestKafkaStream(jsc, params, topic);
            largestKafkaStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
                @Override
                public void call(JavaPairRDD<String, String> rdd) throws Exception {
                    JavaRDD<String> tmp = rdd.map(new Function<Tuple2<String, String>, String>() {
                        @Override
                        public String call(Tuple2<String, String> tp) throws Exception {
                            return tp._2;
                        }
                    });
                    tmp.foreachPartition(new ActionFunction());
//                    new manager().saveOffSets(zkClient,zkPath,rdd);

                }
            });
        } else {
            JavaInputDStream<String> existsKafkaStream = createExistsKafkaStream(jsc, params, readOffsets);
            existsKafkaStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                @Override
                public void call(JavaRDD<String> rdd) throws Exception {
                    rdd.foreachPartition(new ActionFunction());
//                    new OffsetManager().saveOffSets(zkClient,zkPath,rdd);
                }
            });

        }


        return jsc;
    }

    /**
     * -创建kafka stream,如果存在offset，则从offset进行消费，
     * -如果kafka有新增分区，程序会自动扩容，以新kafka分区对应的消费数进行消费
     *
     * @param jsc    -javaSparkStreaming对象
     * @param params -kafka相关参数
     * @param topic  -已经存在的topic
     * @return -返回创建好的对象
     */
    private JavaPairInputDStream<String, String> createLargestKafkaStream(
            JavaStreamingContext jsc,
            HashMap<String, String> params,
            String topic) {
        log.warn("系统第一次启动，没有读取到偏移量，默认从最新的offset消费");
        HashSet<String> topics = new HashSet<>();
        topics.add(topic);
        return KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                params,
                topics);
    }

    /**
     * -创建kafka stream,如果存在offset，则从offset进行消费，
     * -如果kafka有新增分区，程序会自动扩容，以新kafka分区对应的消费数进行消费
     *
     * @param jsc         -javaSparkStreaming对象
     * @param params      -kafka相关参数
     * @param readOffsets -已经存在的topic,partition,offset
     * @return -返回创建好的对象
     */
    private JavaInputDStream<String> createExistsKafkaStream(
            JavaStreamingContext jsc,
            HashMap<String, String> params,
            Map<TopicAndPartition, Long> readOffsets) {
        log.info("系统从zk中读取到偏移量，从上次的偏移量开始消费.....");

        return KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                params,
                readOffsets,
                new Function<MessageAndMetadata<String, String>, String>() {
                    @Override
                    public String call(MessageAndMetadata<String, String> s) throws Exception {
                        return s.message();
                    }
                }
        );
    }
}
