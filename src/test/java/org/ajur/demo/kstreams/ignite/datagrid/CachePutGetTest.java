package org.ajur.demo.kstreams.ignite.datagrid;

import java.util.HashMap;
import java.util.Map;

import org.ajur.demo.kstreams.ignite.utils.Utils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;


public class CachePutGetTest {

    /** Cache name. */
    private static final String CACHE_NAME = "CachePutGetTest_cache";
    // -DIGNITE_H2_DEBUG_CONSOLE=true

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException, InterruptedException {

        try (Ignite ignite = Ignition.start(Utils.igniteConfig())) {

            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                // Individual puts and gets.
                putGet(cache);

                // Bulk puts and gets.
                putAllGetAll(cache);

                Thread.sleep(5 * 1000);
            }
        }

    }

    /**
     * Execute individual puts and gets.
     *
     * @throws IgniteException If failed.
     */
    private static void putGet(IgniteCache<Integer, String> cache) throws IgniteException {
        System.out.println();
        System.out.println(">>> Cache put-get example started.");

        final int keyCnt = 20;

        // Store keys in cache.
        for (int i = 0; i < keyCnt; i++)
            cache.put(i, Integer.toString(i));

        System.out.println(">>> Stored values in cache.");

        for (int i = 0; i < keyCnt; i++)
            System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');
    }

    /**
     * Execute bulk {@code putAll(...)} and {@code getAll(...)} operations.
     *
     * @throws IgniteException If failed.
     */
    private static void putAllGetAll(IgniteCache<Integer, String> cache) throws IgniteException {
        System.out.println();
        System.out.println(">>> Starting putAll-getAll example.");

        final int keyCnt = 20;

        // Create batch.
        Map<Integer, String> batch = new HashMap<>();

        for (int i = 0; i < keyCnt; i++)
            batch.put(i, "bulk-" + Integer.toString(i));

        // Bulk-store entries in cache.
        cache.putAll(batch);

        System.out.println(">>> Bulk-stored values in cache.");

        // Bulk-get values from cache.
        Map<Integer, String> vals = cache.getAll(batch.keySet());

        for (Map.Entry<Integer, String> e : vals.entrySet())
            System.out.println("Got entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
    }

}
