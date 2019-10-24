package com.wjq.kafka;

import com.alibaba.fastjson.JSONArray;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
/**
 * 类名称: KafkaProudce
 * 类描述:
 *
 * @author 武建谦
 * @Time 2019/10/22 18:09
 */
public class ProducerUtil {
    private static KafkaProducer<String, String> kafkaProducer;
    private final static Logger logger = LoggerFactory.getLogger(ProducerUtil.class);

    private ProducerUtil() {

    }

    private static class LazyHandler {

        private static final ProducerUtil instance = new ProducerUtil();
    }

    public static final ProducerUtil getInstance() {
        return LazyHandler.instance;
    }

    public void init() throws ExecutionException, InterruptedException {
        if (kafkaProducer == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers","");
            //这个配置可以设定发送消息后是否需要Broker端返回确认
            //0: 不需要进行确认，速度最快。存在丢失数据的风险。
            //1: 仅需要Leader进行确认，不需要ISR进行确认。是一种效率和安全折中的方式。
            //all: 需要ISR中所有的Replica给予接收确认，速度最慢，安全性最高，但是由于ISR可能会缩小到仅包含一个Replica，所以设置参数为all并不能一定避免数据丢失。
            props.put("acks", "1");
            //重新发送消息次数，到达次数返回错误
            props.put("retries", 0);
            //Producer会尝试去把发往同一个Partition的多个Requests进行合并，batch.size指明了一次Batch合并后Requests总大小的上限。如果这个值设置的太小，可能会导致所有的Request都不进行Batch。
            props.put("batch.size", 163840);
            //Producer默认会把两次发送时间间隔内收集到的所有Requests进行一次聚合然后再发送，以此提高吞吐量，而linger.ms则更进一步，这个参数为每次发送增加一些delay，以此来聚合更多的Message。
            props.put("linger.ms", 1);
            //在Producer端用来存放尚未发送出去的Message的缓冲区大小
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("request.timeout.ms", "60000");
            kafkaProducer = new KafkaProducer<>(props);
        }
    }

    public void sendKakfaMessage(JSONArray jsonarray) throws ExecutionException, InterruptedException {
        for (Object object : jsonarray) {
            String json = object.toString();
            String topic = "topic";
            //通过时间做轮循，均匀分布设置的partition，提高效率。
            int partition = (int) (System.currentTimeMillis() % 3);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, UUID.randomUUID().toString(), json);
            kafkaProducer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Send topic: " + topic + ", partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
                } else {
                    logger.error("Send topic: " + topic + " failed");
                }
            }).get();
        }
    }
}