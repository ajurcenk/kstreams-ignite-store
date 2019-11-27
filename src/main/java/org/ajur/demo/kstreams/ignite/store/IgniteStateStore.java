package org.ajur.demo.kstreams.ignite.store;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;

public  class IgniteStateStore<K,V> implements StateStore, IgniteWritableStore<K,V> {

    // Default store name
    private final String storeName;

    // Ignite state store cache
    final private IgniteCache<K, V> igniteStateCache;

    protected IgniteStateStore(final IgniteCache<K, V> igniteStateCache, final String storeName) {

        this.igniteStateCache = igniteStateCache;
        this.storeName = storeName;
    }


    @Override
    public V read(K key) {

        V value = this.igniteStateCache.get(key);

        return value;
    }

    @Override
    public void write(K key, V value) {

        this.igniteStateCache.put(key, value);
    }

    @Override
    public String name() {
        return this.storeName;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

        // Registers the store
        context.register(this, (key, value) -> {
           // Store is not supports restore logic. Ignore it
        });
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {

        return false;
    }

    @Override
    public boolean isOpen() {

        return true;
    }

}
