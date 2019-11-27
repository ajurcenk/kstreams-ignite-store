package org.ajur.demo.kstreams.ignite.store;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Map;

public class IgniteStoreBuilder<K,V> implements StoreBuilder<IgniteStateStore<K,V>> {

    private final String storeName;

    // Ignite instance
    private Ignite ignite;

    // Cache configuration
    private CacheConfiguration<K, V> igniteCacheCfg;

    // Ignite cache
    private IgniteCache<K,V> igniteCache;


    public IgniteStoreBuilder(final String storeName)
    {
        this.storeName = storeName;
    }

    public IgniteStoreBuilder<K,V> ignite(Ignite ignite)
    {
        this.ignite = ignite;
        return this;
    }

    public IgniteStoreBuilder<K,V> igniteCache(IgniteCache<K, V> igniteCache)
    {
        this.igniteCache = igniteCache;
        return this;
    }

    public StoreBuilder<IgniteStateStore<K, V>> igniteCacheConfig(CacheConfiguration<K, V> cacheCfg)
    {
        this.igniteCacheCfg = cacheCfg;
        return this;
    }

    @Override
    public StoreBuilder<IgniteStateStore<K, V>> withCachingEnabled()
    {
        throw new IllegalArgumentException("withCachingEnabled is not supported");
    }

    @Override
    public StoreBuilder<IgniteStateStore<K, V>> withCachingDisabled() {

        return this;
    }

    @Override
    public StoreBuilder<IgniteStateStore<K, V>> withLoggingEnabled(Map<String, String> config) {

        throw new IllegalArgumentException("withLoggingEnabled is not supported");
    }

    @Override
    public StoreBuilder<IgniteStateStore<K, V>> withLoggingDisabled() {

        throw new IllegalArgumentException("withLoggingDisabled is not supported");
    }

    @Override
    public IgniteStateStore<K, V> build() {

        if (this.igniteCache != null) {

            // Use builder cache
            final IgniteStateStore igniteStore = new IgniteStateStore(this.igniteCache, storeName);

            return  igniteStore;
        }

        // Create Ignite cache

        if (this.ignite == null) {

            throw new IllegalStateException("ignite is not set");
        }

        if (this.igniteCacheCfg == null) {

            throw new IllegalStateException("igniteCacheCfg is not set");
        }

        final IgniteCache<K, V> igniteStateCache = this.ignite.getOrCreateCache(this.igniteCacheCfg);
        final IgniteStateStore igniteStore = new IgniteStateStore(igniteStateCache, storeName);

        return igniteStore;
    }

    @Override
    public Map<String, String> logConfig() {

        return null;
    }

    @Override
    public boolean loggingEnabled() {

        return false;
    }

    @Override
    public String name() {

        return storeName;
    }


}
