import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import kafka.utils.ZkUtils$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.data.Stat;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.Map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OffsetManager implements Serializable {
    private static final Logger log = LogManager.getLogger("OffsetManager");

    /**
     * -读取zk里面的偏移量，如果有就返回对应的分区和偏移量，如果没有就返回
     *
     * @param zkClient      -zk连接的client
     * @param zkOffSetsPath -偏移量路径
     * @param topic-        topic名字
     * @return -偏移量map 或者null
     */
    public HashMap<TopicAndPartition, Long> readOffsets(ZkClient zkClient, String zkOffSetsPath, String topic) {
        ZkUtils$ zkUtils$ = ZkUtils$.MODULE$;
        Tuple2<String, Stat> tuple2 = zkUtils$.readData(zkClient, zkOffSetsPath);
        String offsetsRangesStr = tuple2._1;
        //当前方式获取的str包含引号
        if(offsetsRangesStr.equals("\"\"")){
            log.warn("zk目录下没有发现kafka分区信息！！");
            return null;
        }
        ArrayList<String> topics = new ArrayList<>();
        topics.add(topic);
        Buffer<String> seqTopics = JavaConversions.asScalaBuffer(topics);
        Map<String, Seq<Object>> partitionsForTopics = zkUtils$.getPartitionsForTopics(zkClient, seqTopics);
        Seq<Object> objectSeq = partitionsForTopics.get(topic).get();
        List<Object> latest_partitions = JavaConversions.seqAsJavaList(objectSeq);
        HashMap<TopicAndPartition, Long> offsetMap = new HashMap<>();
        String[] offsets = offsetsRangesStr.split(",");
        for (String offset : offsets) {
            String[] partitionAndOffset = offset.split(":");
            TopicAndPartition tap = new TopicAndPartition(topic, Integer.parseInt(partitionAndOffset[0]));
            offsetMap.put(tap, Long.parseLong(partitionAndOffset[1]));
        }
        if (latest_partitions.size() > 0) {
            if (latest_partitions.size() > offsetMap.size()) {
                log.warn("发现kafka新增分区！！！");
                int begin = latest_partitions.size();
                int last = offsetMap.size();
                for (int i = last; i < begin; i++) {
                    TopicAndPartition tap = new TopicAndPartition(topic, i);
                    offsetMap.put(tap, 0L);
                }
                log.warn("新增分区个数为：" + (begin - last));
            } else {
                log.warn("没有发现新增的kafka分区！");
            }
            return offsetMap;
        } else {
            log.warn("没有发现此topic的kafka分区信息： topic为：" + topic);
            return null;
        }
    }


    /**
     * 保存每个批次的rdd的offset到zk
     *
     * @param zkClient     zk连接的client
     * @param zkOffSetPath 偏移量路径
     * @param rdd          每个批次的数据
	 * @param Pairrdd      批次的数据
     */
    public void saveOffSets(ZkClient zkClient, String zkOffSetPath, JavaRDD<String> rdd,JavaPairRDD<String, String> Pairrdd) {
		OffsetRange[] offsets;
		if(rdd != null){
			offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
		}else if(Pairrdd != null){
			offsets = ((HasOffsetRanges) Pairrdd.rdd()).offsetRanges();
		}else{
			log.debug("未正确传递rdd，请检查代码");
			return;
		}
        StringBuilder sb = new StringBuilder();
        for (OffsetRange offset : offsets) {
            int partition = offset.partition();
            long untilOffset = offset.untilOffset();
            sb.append(partition).append(":").append(untilOffset).append(",");
        }
        String offsetsRangesStr = sb.toString().substring(0, sb.length()-1);
        log.debug("保存的偏移量：" + offsetsRangesStr);
        ZkUtils.updatePersistentPath(zkClient, zkOffSetPath, offsetsRangesStr);
    }
}
