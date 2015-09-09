package utils.embeddedkafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.zookeeper.server.ZooKeeperLocal;

import java.io.IOException;
import java.util.Properties;

/**
 * Local embedded Kafka. Taken from https://gist.github.com/fjavieralba/7930018
 */
public class KafkaLocal {

    public KafkaServerStartable kafka;
    public ZooKeeperLocal zookeeper;

    public KafkaLocal(Properties kafkaProperties, Properties zkProperties) throws IOException, InterruptedException{
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

        //start local zookeeper
        System.out.println("starting local zookeeper...");
        zookeeper = new ZooKeeperLocal(zkProperties);
        System.out.println("done");
        //start local kafka broker
        kafka = new KafkaServerStartable(kafkaConfig);
        System.out.println("starting local kafka broker...");
        kafka.startup();
        System.out.println("done");
    }


    public void stop(){
        //stop kafka broker
        System.out.println("stopping kafka...");
        kafka.shutdown();
        zookeeper.shutdown();
        System.out.println("done");
    }

}