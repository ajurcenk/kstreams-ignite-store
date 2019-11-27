package org.ajur.demo.kstreams.ignite.app;

import org.ajur.demo.kstreams.ignite.model.Person;
import org.ajur.demo.kstreams.ignite.processors.CountingProcessorSupplier;
import org.ajur.demo.kstreams.ignite.store.IgniteStateStore;
import org.ajur.demo.kstreams.ignite.store.IgniteStoreBuilder;
import org.ajur.demo.kstreams.ignite.store.IgniteWritableStore;
import org.ajur.demo.kstreams.ignite.utils.Utils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class App {

    private final static String STORE_NAME = "wordcount-store";
    public static final String SOURCE_TOPIC = "wordcount-source-topic";
    public static final String TARGET_TOPIC = "wordcount-target-topic";

    private  Ignite ignite;
    private  IgniteCache<String,Long> igniteWordCountCache;


    public static void main(String[] args) throws Exception {

            System.out.println("Staring application");

            final App app = new App();
            app.init();

            app.start();

            System.out.println("Application is started");

    }



    private Properties streamsConfiguration() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return props;
    }

    protected void init() {

        // Create Ignite instance and cache
        this.ignite = Ignition.start(Utils.igniteConfig());

        // Create cache
        this.igniteWordCountCache = ignite.getOrCreateCache(STORE_NAME);

    }

    protected void start() {

        // Create ignite state store
        final IgniteStoreBuilder<String, Long> igniteStoreBuilder = new IgniteStoreBuilder<String,Long>(CountingProcessorSupplier.STORE_NAME);
        igniteStoreBuilder
                .igniteCache(igniteWordCountCache)
                .ignite(ignite);


        // Create application topology
        final Topology topology = new Topology();

        topology.addSource("Source", SOURCE_TOPIC)
                .addProcessor("Process", new CountingProcessorSupplier(), "Source")
                .addStateStore(igniteStoreBuilder, "Process")
                .addSink("Sink", TARGET_TOPIC, new StringSerializer(), new LongSerializer(), "Process");

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration());

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();

            ignite.close();

            System.out.println("Application is stopped");
        }));

        streams.cleanUp();

        // Start streaming tasks
        streams.start();
    }

}
