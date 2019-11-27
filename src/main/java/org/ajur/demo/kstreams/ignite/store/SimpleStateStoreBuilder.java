package org.ajur.demo.kstreams.ignite.store;

import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Map;


public class SimpleStateStoreBuilder implements StoreBuilder<SimpleStateStore> {
    @Override
    public StoreBuilder<SimpleStateStore> withCachingEnabled() {
        return this;
    }

    @Override
    public StoreBuilder<SimpleStateStore> withCachingDisabled() {
        return this;
    }

    @Override
    public StoreBuilder<SimpleStateStore> withLoggingEnabled(Map<String, String> config) {
        return this;
    }

    @Override
    public StoreBuilder<SimpleStateStore> withLoggingDisabled() {
        return this;
    }

    @Override
    public SimpleStateStore build() {
        return new SimpleStateStore();
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
        return "hello";
    }
}
