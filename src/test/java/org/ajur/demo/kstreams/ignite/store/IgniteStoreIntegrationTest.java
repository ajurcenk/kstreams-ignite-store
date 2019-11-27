package org.ajur.demo.kstreams.ignite.store;


import kafka.utils.MockTime;
import org.ajur.demo.kstreams.ignite.processors.CountingProcessorSupplier;
import org.ajur.demo.kstreams.ignite.utils.Utils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;

import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class IgniteStoreIntegrationTest {

    private static final int NUM_BROKERS = 1;
    public static final String SOURCE_TOPIC = "source-topic";
    public static final String TARGET_TOPIC = "target-topic";



    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);


    private final MockTime mockTime = CLUSTER.time;

    private static Ignite ignite;
    private static IgniteCache<String,Long> igniteStoreCache;

    @BeforeClass
    public static void before() throws Exception {

        // Create topics
        CLUSTER.createTopic(SOURCE_TOPIC);
        CLUSTER.createTopic(TARGET_TOPIC);

        // Create Ignite instance and cache
        ignite = Ignition.start(Utils.igniteConfig());

        // Create cache
        igniteStoreCache = ignite.getOrCreateCache(CountingProcessorSupplier.STORE_NAME);

    }

    private Properties streamsConfiguration() {

        final String applicationId = "testApp";

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return streamsConfiguration;
    }

    private Properties producerConfiguration() {

        final Properties cfg = new Properties();

        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");


        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return cfg;
    }


    private Properties consumerConfiguration() {

        final Properties cfg = new Properties();

        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "test-grouo-1");

        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        return cfg;
    }

    @AfterClass
    public static void  after() {

        // Stop ignite
        ignite.close();
    }

    @Test
    public void testWordCount() throws ExecutionException, InterruptedException {

        // Create ignite state store
        final IgniteStoreBuilder<String, Long> igniteStoreBuilder = new IgniteStoreBuilder<String,Long>(CountingProcessorSupplier.STORE_NAME);
        igniteStoreBuilder
                .igniteCache(igniteStoreCache)
                .ignite(ignite);


        // Create application topology
        Topology topology = new Topology();

        topology.addSource("Source", SOURCE_TOPIC)
                .addProcessor("Process", new CountingProcessorSupplier(), "Source")
                .addStateStore(igniteStoreBuilder, "Process")
                .addSink("Sink", TARGET_TOPIC, new StringSerializer(), new LongSerializer(), "Process");

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration());

        streams.cleanUp();

        // Start streaming tasks
        streams.start();

        // Source data
        final List<String> inputData = new ArrayList<>();
        inputData.add("Vilnius");
        inputData.add("Kaunas");
        inputData.add("Vilnius");

        // Expected output
        final List<KeyValue<String, Long>> expectedRecords  = new ArrayList<>();
        expectedRecords.add(new KeyValue("Vilnius", 1L));
        expectedRecords.add(new KeyValue("Kaunas", 1L));
        expectedRecords.add(new KeyValue("Vilnius", 2L));

        IntegrationTestUtils.produceValuesSynchronously(SOURCE_TOPIC, inputData, producerConfiguration(), mockTime);

        // Get data from target topic as string
        final List<KeyValue<String, String>> testOut = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfiguration(), TARGET_TOPIC, 3);

       final List<KeyValue<String, Long>> testOutConverted = new ArrayList<>();

       // Convert data from target topic to Loong
       for (KeyValue<String, String> rec : testOut) {
            final Long count = new LongDeserializer().deserialize("", rec.value.getBytes());
            testOutConverted.add(new KeyValue<>(rec.key, count));
        }

        // Compare output results
        Assert.assertTrue(testOutConverted.containsAll(expectedRecords));

        streams.close();

        // Check ignite store,
        Assert.assertTrue( igniteStoreCache.get("Vilnius") == 2L);
        Assert.assertTrue( igniteStoreCache.get("Kaunas") == 1L);
    }
}
