package org.ajur.demo.kstreams.ignite.store;

import org.ajur.demo.kstreams.ignite.processors.CountingProcessorSupplier;
import org.ajur.demo.kstreams.ignite.utils.Utils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.Map;
import java.util.Properties;


/**
 * Ignite store test case
 */
public class IgniteStateStoreTest {

    public static final String SOURCE_TOPIC = "source-topic";
    public static final String TARGET_TOPIC = "target-topic";

    private static final String CACHE_NAME = "ignite-kstreams-test-cache";
    private static final String COUNTING_CACHE_NAME = "ignite-kstreams-counting-test-cache";


    private static final String STORE_NAME = "test-ignite-store";


    private Ignite ignite;
    private IgniteCache<String,String> igniteStoreCache;
    private IgniteCache<String,Long> igniteCountingStoreCache;

    @Before
    public void before() throws Exception {


        // Start ignite
        this.ignite = Ignition.start(Utils.igniteConfig());

        // Create cache
        this.igniteStoreCache = ignite.getOrCreateCache(CACHE_NAME);
        this.igniteCountingStoreCache = ignite.getOrCreateCache(COUNTING_CACHE_NAME);

    }

    @After
    public void after() {

        // Stop ignite
        this.ignite.close();
    }

    @Test
    public void testIgniteCacheExists() {

        long testCacheCount = this.ignite.cacheNames().stream().filter(n->n.equals(CACHE_NAME)).count();

        Assert.assertTrue(testCacheCount == 1);
    }


    private Properties configure() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggrApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "foo:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }


    /**
     * Tests store registration
     */
    @Test
    public  void storeRegistrationTest() {

        // Create ignite state store
        final IgniteStoreBuilder<String, String> igniteStoreBuilder = new IgniteStoreBuilder<String,String>(STORE_NAME);
        igniteStoreBuilder
                .igniteCache(this.igniteStoreCache)
                .ignite(this.ignite);

        // Create topology
        final StreamsBuilder builder = new StreamsBuilder();

        builder
                .addStateStore(igniteStoreBuilder)
                .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .transform(new SimpleIgniteTransfomerSupplier(igniteStoreBuilder.name()), igniteStoreBuilder.name())
                .to(TARGET_TOPIC);

        final TopologyTestDriver testTopology = new TopologyTestDriver(builder.build(), configure());

        Map<String, StateStore> allStores =  testTopology.getAllStateStores();

        Assert.assertTrue(testTopology.getStateStore(igniteStoreBuilder.name()) != null);

    }

    @Test
    public  void storeReadWriteOpsTest() {

        // Create ignite state store
        final IgniteStoreBuilder<String, String> igniteStoreBuilder = new IgniteStoreBuilder<String,String>(STORE_NAME);

        igniteStoreBuilder
                .igniteCache(this.igniteStoreCache)
                .ignite(this.ignite);

        // Create topology
        final StreamsBuilder builder = new StreamsBuilder();

        builder
                .addStateStore(igniteStoreBuilder)
                .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .transform(new SimpleIgniteTransfomerSupplier(igniteStoreBuilder.name()), igniteStoreBuilder.name())
                .to(TARGET_TOPIC);

        final TopologyTestDriver testTopologyDriver = new TopologyTestDriver(builder.build(), configure());

        Map<String, StateStore> allStores =  testTopologyDriver.getAllStateStores();

        Assert.assertTrue(testTopologyDriver.getStateStore(igniteStoreBuilder.name()) != null);

        // Add state to the store
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(SOURCE_TOPIC, new StringSerializer(), new StringSerializer());

        testTopologyDriver.pipeInput(factory.create(SOURCE_TOPIC, "one", "Item one"));
        testTopologyDriver.pipeInput(factory.create(SOURCE_TOPIC, "two", "Item two"));

        final IgniteStateStore store = (IgniteStateStore) testTopologyDriver.getStateStore(STORE_NAME);

        Assert.assertNotNull( store.read("one"));
        Assert.assertNotNull( store.read("two"));
        Assert.assertNull( store.read("five"));
    }


    @Test
    public  void countingProcessorTest() {

        // Create ignite state store
        final IgniteStoreBuilder<String, Long> igniteStoreBuilder = new IgniteStoreBuilder<String,Long>(CountingProcessorSupplier.STORE_NAME);

        igniteStoreBuilder
                .igniteCache(this.igniteCountingStoreCache)
                .ignite(this.ignite);

        // Create topology
        final StreamsBuilder builder = new StreamsBuilder();

        builder
                .addStateStore(igniteStoreBuilder)
                .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .process(new CountingProcessorSupplier(), CountingProcessorSupplier.STORE_NAME);


        final TopologyTestDriver testTopologyDriver = new TopologyTestDriver(builder.build(), configure());

        Map<String, StateStore> allStores =  testTopologyDriver.getAllStateStores();

        Assert.assertTrue(testTopologyDriver.getStateStore(igniteStoreBuilder.name()) != null);

        // Add state to the store
       ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(SOURCE_TOPIC, new StringSerializer(), new StringSerializer());

        testTopologyDriver.pipeInput(factory.create(SOURCE_TOPIC, "one", "Item one"));
        testTopologyDriver.pipeInput(factory.create(SOURCE_TOPIC, "two", "Item two"));

        final IgniteStateStore<String, Long> store = (IgniteStateStore) testTopologyDriver.getStateStore(CountingProcessorSupplier.STORE_NAME);

         Assert.assertNotNull( store.read("one"));
         Assert.assertNotNull( store.read("two"));
         Assert.assertNull( store.read("five"));

         Assert.assertTrue( store.read("one") == 1L);
         Assert.assertTrue( store.read("Item") == 2L);
    }

}


