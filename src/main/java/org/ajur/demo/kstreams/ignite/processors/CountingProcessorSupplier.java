package org.ajur.demo.kstreams.ignite.processors;

import org.ajur.demo.kstreams.ignite.store.IgniteStateStore;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class CountingProcessorSupplier implements ProcessorSupplier<String, String> {


    public static String STORE_NAME = "wordCount_store";

    @Override
    public Processor<String, String> get() {
        return new CountingProcessor();
    }


    private static final class CountingProcessor implements Processor<String, String> {

        private final Logger logger = LoggerFactory.getLogger(CountingProcessor.class);


        private ProcessorContext context;
        private IgniteStateStore<String, Long> store;


        public CountingProcessor() {

        }

        @Override
        public void init(ProcessorContext processorContext) {

            this.context = processorContext;

            this.store = (IgniteStateStore<String, Long>) context.getStateStore(STORE_NAME);

            if (this.store == null) {

                throw new IllegalStateException("Store: " + STORE_NAME + " is not found");
            }
        }

        @Override
        public void process(final String key, String value) {

            // Splitting logic
            String[] values = value.split("\\s+");

            Arrays.stream(values).forEach(word -> {

                logger.info("Start processing word: {}", word);

                if (this.store.read(word) == null) {
                    // First time
                    this.store.write(word, 0L);
                }
                // Increment counter
                final long counter = this.store.read(word) + 1;
                this.store.write(word, counter);

                context.forward(word, counter);

                logger.info("End processing word: {} count: {} ", word , this.store.read(word));

            });

        }

        @Override
        public void close() {

        }
    }
}