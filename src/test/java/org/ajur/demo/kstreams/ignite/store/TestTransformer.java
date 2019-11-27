package org.ajur.demo.kstreams.ignite.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class TestTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private static final String STORE_NAME = "test-ignite-store";

    // Ignite state store
    private IgniteStateStore<String, String > store;

    @Override
    public void init(ProcessorContext context) {

        this.store = (IgniteStateStore) context.getStateStore(STORE_NAME);

        if (this.store == null) {

            throw new IllegalStateException("store in not exists");
        }
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {

        this.store.write(key, value.toLowerCase());

        return new KeyValue<>(key, value.toLowerCase());

    }

    @Override
    public void close() {

    }
}
