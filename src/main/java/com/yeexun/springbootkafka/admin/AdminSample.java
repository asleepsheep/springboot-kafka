package com.yeexun.springbootkafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.ConfigResource;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/5/30 14:33
 */
public class AdminSample {

    private final static String TOPIC_NAME = "jiangfan-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        AdminClient adminClient = AdminSample.adminClient();
//        System.out.println(adminClient);
////        创建topic实例
//        createTopic();

////        删除topic
//        delTopics();
////        获取topic列表
//        topicLists();
//
        //获取topic的描述
        describeTopics();

        //添加partition
        createPartition();



////        //修改config
//        alterConfig();

////        //查看config
//        describeConfig();

        //查询所有group
//        getAllGroupsForTopic();
    }

    /**
     * 获取topic列表
     */
    public static void topicLists() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

        //是否查看internal选项
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);

//        ListTopicsResult listTopicsResult = adminClient.listTopics();
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);

        Set<String> names = listTopicsResult.names().get();
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();

        //打印names
        names.stream().forEach(System.out::println);

        //打印topicListings
        topicListings.stream().forEach(topicListing -> {
            System.out.println(topicListing);
        });
    }

    /**
     * 创建topic实例
     */
    public static void createTopic() {
        AdminClient adminClient = adminClient();
        CreateTopicsResult result = null;
        try {
            // 创建topic前，可以先检查topic是否存在，如果已经存在，则不用再创建了
            Set<String> topics = adminClient.listTopics().names().get();
            if (topics.contains(TOPIC_NAME)) {
                return;
            }

            // 创建topic
            NewTopic newTopic = new NewTopic(TOPIC_NAME, 3, (short) 1);
            result = adminClient.createTopics(Collections.singletonList(newTopic));
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        System.out.println("CreateTopicsResult:" + result);
    }


    /**
     * 删除topics
     */
    public static void delTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));

        deleteTopicsResult.all().get();
    }

    /**
     * 描述topics :
     * name=test,
     * internal=false,
     * partitions=(
     * partition=0,
     * leader=localhost:9092
     * (id: 0 rack: null),
     * replicas=localhost:9092
     * (id: 0 rack: null),
     * isr=localhost:9092
     * (id: 0 rack: null)),
     * <p>
     * (partition=1, leader=localhost:9092 (id: 0 rack: null), replicas=localhost:9092 (id: 0 rack: null), isr=localhost:9092 (id: 0 rack: null)),(partition=2, leader=localhost:9092 (id: 0 rack: null), replicas=localhost:9092 (id: 0 rack: null), isr=localhost:9092 (id: 0 rack: null)),(partition=3, leader=localhost:9092 (id: 0 rack: null), replicas=localhost:9092 (id: 0 rack: null), isr=localhost:9092 (id: 0 rack: null)),(partition=4, leader=localhost:9092 (id: 0 rack: null), replicas=localhost:9092 (id: 0 rack: null), isr=localhost:9092 (id: 0 rack: null)), authorizedOperations=null)
     */
    public static void describeTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult test = adminClient.describeTopics(Arrays.asList("test"));

        Map<String, TopicDescription> stringTopicDescriptionMap = test.all().get();

        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();

        entries.stream().forEach((entry) -> System.out.println("topic名称:" + entry.getKey() + ", 描述:" + entry.getValue()));
    }

    /**
     * 查看配置信息
     * <p>
     * configResource :ConfigResource(type=TOPIC, name='test')config: Config(entries=[ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.format.version, value=3.0-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1048588, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])
     */
    public static void describeConfig() {
        AdminClient adminClient = adminClient();

        //TODO 这里做个预留,集群时会讲到
//        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);

        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = null;
        try {
            configResourceConfigMap = describeConfigsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        configResourceConfigMap.entrySet().stream().forEach((entry) -> {
            System.out.println("configResource :" + entry.getKey() + "config: " + entry.getValue());
        });
    }

    /**
     * 修改config信息
     *
     * @throws Exception
     */
    public static void alterConfig() {
        AdminClient adminClient = adminClient();
        Map<ConfigResource, Config> configMaps = new HashMap<>();

        //组织两个参数
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate", "true")));

        configMaps.put(configResource, config);

        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configMaps);
        try {
            alterConfigsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    /**
     * 设置AdminClient
     *
     * @return
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        AdminClient adminClient = AdminClient.create(properties);

        return adminClient;
    }

    /**
     * 查询topic的所有group
     *

     * @return
     */
    public static void getAllGroupsForTopic() {
        AdminClient client = adminClient();

        ListConsumerGroupsResult listConsumerGroupsResult = client.listConsumerGroups();

        try {
            Collection<ConsumerGroupListing> consumerGroupListings = listConsumerGroupsResult.all().get();

            consumerGroupListings.stream().forEach(consumerGroupListing -> System.out.println("group为:" + consumerGroupListing.toString()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    /**
     * 创建新的partition
     */
    public static void createPartition() throws ExecutionException, InterruptedException {
        AdminClient client = adminClient();

        NewPartitions newPartitionRequest = NewPartitions.increaseTo(2);

        client.createPartitions(Collections.singletonMap(TOPIC_NAME, newPartitionRequest)).all().get();
    }
}
