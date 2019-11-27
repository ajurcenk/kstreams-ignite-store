/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ajur.demo.kstreams.ignite.app;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;

import org.ajur.demo.kstreams.ignite.utils.Utils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;


public class IgniteContinuousQueryApp {

    /** Cache name. */
    private static final String CACHE_NAME = "wordcount-store";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {

        try (Ignite ignite = Ignition.start(Utils.igniteConfig())) {

            System.out.println();
            System.out.println(">>> Cache continuous query example started.");

            // Auto-close cache at the end of the example.


            try (IgniteCache<String, Long> cache = ignite.getOrCreateCache(CACHE_NAME)) {


                // Create new continuous query.
                ContinuousQuery<String, Long> qry = new ContinuousQuery<>();

                qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<String, Long>() {

                    @Override public boolean apply(String key, Long val) {

                        return val > 1;
                    }
                }));

                // Callback that is called locally when update notifications are received.
                qry.setLocalListener(new CacheEntryUpdatedListener<String, Long>() {

                    @Override public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends Long>> evts) {

                        for (CacheEntryEvent<? extends String, ? extends Long> e : evts)

                            System.out.println("Updated entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                    }
                });

                // This filter will be evaluated remotely on all nodes.
                // Entry that pass this filter will be sent to the caller.
                qry.setRemoteFilterFactory(new Factory<CacheEntryEventFilter<String, Long>>() {

                    @Override public CacheEntryEventFilter<String, Long> create() {

                        return new CacheEntryFilter();
                    }
                });

                // Execute query.
                try (QueryCursor<Cache.Entry<String, Long>> cur = cache.query(qry)) {

                    // Iterate through existing data.
                    for (Cache.Entry<String, Long> e : cur) {

                        System.out.println("Queried existing entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                    }


                    boolean done = false;

                    while (!done) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            done = true;
                        }
                    }
                }
            }
            finally {

            }
        }

    }

    /**
     * Filter returns {@code true} for entries which have key bigger than 10.
     */
    @IgniteAsyncCallback
    private static class CacheEntryFilter implements CacheEntryEventFilter<String, Long> {

        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends String, ? extends Long> e) {

            return e.getValue() > 1L;
        }
    }
}
