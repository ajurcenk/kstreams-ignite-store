package org.ajur.demo.kstreams.ignite.store;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;

public class SimpleStateStore implements StateStore {

    @Override
    public String name() {
        return "hello";
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

        context.register(this, (key, value) -> {
            // here the store restore should happen from the changelog topic.
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
        return true;
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}
