package org.ajur.demo.kstreams.ignite.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;


public class SimpleIgniteTransfomerSupplier implements TransformerSupplier<String, String, KeyValue<String, String>> {


    final private String stateStoreName;


    public SimpleIgniteTransfomerSupplier(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public Transformer<String, String, KeyValue<String, String>> get() {
        return new Transformer<String, String, KeyValue<String, String>>() {


            private IgniteStateStore<String, String> stateStore;

            @SuppressWarnings("unchecked")
            @Override
            public void init(ProcessorContext processorContext) {

                this.stateStore = (IgniteStateStore<String, String>) processorContext.getStateStore(stateStoreName);

                if (this.stateStore == null) {

                    throw new IllegalStateException("state store is no found");
                }
            }

            @Override
            public KeyValue<String, String> transform(String key, String value) {

                this.stateStore.write(key, value.toLowerCase());

                return new KeyValue<>(key, value.toLowerCase());
            }

            @Override
            public void close() {
                // No need to close as this is handled by kafka.
            }
        };
    }
}
