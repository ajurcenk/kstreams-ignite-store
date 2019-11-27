package org.ajur.demo.kstreams.ignite.utils;

import java.io.InputStream;

public class Utils {

    private static final String IGNITE_CFG_FILE = "/ignite-basic.xml";

    public static InputStream igniteConfig() {

        return  Utils.class.getResourceAsStream(IGNITE_CFG_FILE);
    }
}
