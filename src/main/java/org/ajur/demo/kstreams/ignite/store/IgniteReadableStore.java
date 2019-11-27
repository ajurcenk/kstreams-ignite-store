package org.ajur.demo.kstreams.ignite.store;

/**
 * Ignite read only operations
 * @param <K>
 * @param <V>
 */
public interface IgniteReadableStore<K,V> {

    /**
     * Reads value by key
     *
     * @param key
     * @return
     */
    V read(K key);
}
