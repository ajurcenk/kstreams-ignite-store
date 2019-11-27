package org.ajur.demo.kstreams.ignite.datagrid;

import javax.cache.Cache;

import org.ajur.demo.kstreams.ignite.model.Organization;
import org.ajur.demo.kstreams.ignite.model.Person;
import org.ajur.demo.kstreams.ignite.utils.Utils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import org.apache.ignite.lang.IgniteBiPredicate;

public class CacheQueryTest {

    private static final String ORG_CACHE = "Organization_cache";
    private static final String PERSON_CACHE = "Person_cache";

    public static void main(String[] args) throws Exception {

        System.out.println();
        System.out.println(">>> Cache query example started.");


        try (Ignite ignite = Ignition.start(Utils.igniteConfig());) {
            System.out.println();
            System.out.println(">>> Cache query example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);

            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<AffinityKey<Long>, Person> personCacheCfg =
                    new CacheConfiguration<>(PERSON_CACHE);

            personCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            personCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);

            try {
                // Create caches.
                ignite.getOrCreateCache(orgCacheCfg);
                ignite.getOrCreateCache(personCacheCfg);

                // Populate caches.
                initialize();

                // Example for SCAN-based query based on a predicate.
                scanQuery();

                // Example for TEXT-based querying for a given string in peoples resumes.
                textQuery();

               // Thread.sleep(1000 * 500);
            }
            finally {
                // Distributed cache could be removed from cluster only by Ignite.destroyCache() call.
                ignite.destroyCache(PERSON_CACHE);
                ignite.destroyCache(ORG_CACHE);
            }

            print("Cache query example finished.");
        }


    }

    /**
     * Example for scan query based on a predicate using binary objects.
     */
    private static void scanQuery() {
        IgniteCache<BinaryObject, BinaryObject> cache = Ignition.ignite()
                .cache(PERSON_CACHE).withKeepBinary();

        ScanQuery<BinaryObject, BinaryObject> scan = new ScanQuery<>(
                new IgniteBiPredicate<BinaryObject, BinaryObject>() {
                    @Override public boolean apply(BinaryObject key, BinaryObject person) {
                        return person.<Double>field("salary") <= 1000;
                    }
                }
        );

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000 (queried with SCAN query): ", cache.query(scan).getAll());
    }


    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     */
    private static void textQuery() {

        IgniteCache<Long, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        //  Query for all people with "Master Degree" in their resumes.
        QueryCursor<Cache.Entry<Long, Person>> masters =
                cache.query(new TextQuery<Long, Person>(Person.class, "Master"));

        // Query for all people with "Bachelor Degree" in their resumes.
        QueryCursor<Cache.Entry<Long, Person>> bachelors =
                cache.query(new TextQuery<Long, Person>(Person.class, "Bachelor"));

        print("Following people have 'Master Degree' in their resumes: ", masters.getAll());
        print("Following people have 'Bachelor Degree' in their resumes: ", bachelors.getAll());
    }


    /**
     * Populate cache with test data.
     */
    private static void initialize() {

        IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE);

        // Clear cache before running the example.
        orgCache.clear();

        // Organizations.
        Organization org1 = new Organization("ApacheIgnite");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.id(), org1);
        orgCache.put(org2.id(), org2);

        IgniteCache<AffinityKey<Long>, Person> colPersonCache = Ignition.ignite().cache(PERSON_CACHE);

        // Clear caches before running the example.
        colPersonCache.clear();

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        colPersonCache.put(p1.key(), p1);
        colPersonCache.put(p2.key(), p2);
        colPersonCache.put(p3.key(), p3);
        colPersonCache.put(p4.key(), p4);
    }

    /**
     * Prints message and query results.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        print(msg);
        print(col);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Prints query results.
     *
     * @param col Query results.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col)
            System.out.println(">>>     " + next);
    }
}
