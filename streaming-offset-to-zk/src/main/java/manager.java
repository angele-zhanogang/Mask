import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.data.Stat;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import java.util.*;

public class manager {
    Logger log = LogManager.getLogger("KafkaOffsetManage");

    /***读取zk里面的偏移量，如果有就返回对应的分区和偏移量
     * 如果没有就返回None
     * @param zkClient  zk连接的client
     * @param zkOffsetPath   偏移量路径
     * @param topic    topic名字
     * @return 偏移量Map or None
     */
    public Map<TopicAndPartition, Long> readOffsets(ZkClient zkClient, String zkOffsetPath, String topic) {
        //保存需要的分许信息和偏移量，用于返回构建kafka createDirectStream
        HashMap<TopicAndPartition, Long> offsets = new HashMap<>();
        //获取zk中保存的分区信息和偏移量
        Option<String> offsetsRangesStrOpt = ZkUtils.readDataMaybeNull(zkClient, zkOffsetPath)._1();
        if (offsetsRangesStrOpt != null) {
            String offsetsRangesStr = offsetsRangesStrOpt.get();
            if (offsetsRangesStr != null) {
                ArrayList<String> list = new ArrayList<>();
                list.add(topic);
                Buffer<String> stringBuffer = JavaConversions.asScalaBuffer(list);
                //从kafka的topic中的获取分区和偏移量信息
                Seq<Object> latest_partitions = ZkUtils.getPartitionsForTopics(zkClient, stringBuffer).get(topic).get();
                List<Object> objects = JavaConversions.seqAsJavaList(latest_partitions);
                String[] split = offsetsRangesStr.split(",");
                for (String s : split) {
                    String[] partitionAndOffset = s.split(":");
                    TopicAndPartition tap = new TopicAndPartition(topic, Integer.parseInt(partitionAndOffset[0]));
                    offsets.put(tap, Long.parseLong(partitionAndOffset[1]));
                }
                if (offsets.size() < latest_partitions.size()) {
                    log.warn("发现kafka新增分区,自动进行分区扩展");
                    for(int i = offsets.size();i<latest_partitions.size();i++){
                        //添加新的kafka分区的offset信息，从0开始
                        offsets.put(new TopicAndPartition(topic,i),0L);
                    }
                }else {
                    log.warn("没有发现新增的kafka分区");
                }
                log.warn("分区信息为："+offsets.toString());
            }
        }
        return offsets;


//                offsetsRangesStrOpt match {
//            case Some(offsetsRangesStr) =>
//                //这个topic在zk里面最新的分区数量
//                val  lastest_partitions= ZkUtils.getPartitionsForTopics(zkClient,Seq(topic)).get(topic).get
//                var offsets = offsetsRangesStr.split(",")//按逗号split成数组
//                        .map(s => s.split(":"))//按冒号拆分每个分区和偏移量
//          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }//加工成最终的格式
//          .toMap//返回一个Map
//
//            //说明有分区扩展了
//            if(offsets.size<lastest_partitions.size){
//                //得到旧的所有分区序号
//                val old_partitions=offsets.keys.map(p=>p.partition).toArray
//                //通过做差集得出来多的分区数量数组
//                val add_partitions=lastest_partitions.diff(old_partitions)
//                if(add_partitions.size>0){
//                    log.warn("发现kafka新增分区："+add_partitions.mkString(","))
//                    add_partitions.foreach(partitionId=>{
//                            offsets += (TopicAndPartition(topic,partitionId)->0)
//                    log.warn("新增分区id："+partitionId+"添加完毕....")
//            })
//
//                }
//
//            }else{
//                log.warn("没有发现新增的kafka分区："+lastest_partitions.mkString(","))
//            }
//
//
//            Some(offsets)//将Map返回
//            case None =>
//                None//如果是null，就返回None
//        }
    }


    //    /****
//     * 保存每个批次的rdd的offset到zk中
//     * @param zkClient zk连接的client
//     * @param zkOffsetPath   偏移量路径
//     * @param rdd     每个批次的rdd
//     */

    /**
     * 保存每个批次的rdd的offset到zk中
     *
     * @param zkClient zk连接的client
     * @param zkPath   偏移量路径
     * @param pairRDD  每个批次的rdd
     * @param rdd      每个批次的rdd
     */
    private void saveOffset(ZkClient zkClient, String zkPath, JavaPairRDD<String, String> pairRDD, JavaRDD<String> rdd) {
        OffsetRange[] offsetRanges = null;
        StringBuilder sb = new StringBuilder();
        if (pairRDD != null) {
            offsetRanges = ((HasOffsetRanges) (pairRDD.rdd())).offsetRanges();
        } else if (rdd != null) {
            offsetRanges = ((HasOffsetRanges) (rdd.rdd())).offsetRanges();
        } else {
            throw new IllegalArgumentException("参数传入不正确");
        }

        for (OffsetRange offsetRange : offsetRanges) {
            sb.append(offsetRange.partition()).append(":").append(offsetRange.untilOffset()).append(",");
        }
        String offstr = sb.toString();
        offstr = offstr.substring(0, offstr.length() - 1);
        ZkUtils.updatePersistentPath(zkClient, zkPath, offstr);

    }
}
