package org.ajur.demo.kstreams.ignite.store;

/**
 * Ignite write operations
 *
 * @param <K>
 * @param <V>
 */
public interface IgniteWritableStore<K,V> extends IgniteReadableStore<K,V> {

    void write(K key, V value);
}
