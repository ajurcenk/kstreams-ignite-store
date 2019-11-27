package org.ajur.demo.kstreams.ignite.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;


public class SimpleTransfomerSupplier implements TransformerSupplier<String, String, KeyValue<String, String>> {


    final private String stateStoreName;


    public SimpleTransfomerSupplier(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public Transformer<String, String, KeyValue<String, String>> get() {
        return new Transformer<String, String, KeyValue<String, String>>() {


            private KeyValueStore<String, Long> stateStore;

            @SuppressWarnings("unchecked")
            @Override
            public void init(ProcessorContext processorContext) {
                stateStore = (KeyValueStore<String, Long>) processorContext.getStateStore(stateStoreName);
            }

            @Override
            public KeyValue<String, String> transform(String key, String value) {
                Long countSoFar = stateStore.get(key);
                if(countSoFar == null){
                    System.out.print("Initializing count so far. this message should be printed only once");
                    countSoFar = 0L;
                }
                countSoFar += value.length();
                System.out.printf(" Key: %s, Value: %s, Count: %d\n\n", key, value, countSoFar);
                stateStore.put(key, countSoFar);
                return KeyValue.pair(key, value);
            }

            @Override
            public void close() {
                // No need to close as this is handled by kafka.
            }
        };
    }
}
