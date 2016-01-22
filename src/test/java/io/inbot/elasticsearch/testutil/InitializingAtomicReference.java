package io.inbot.elasticsearch.testutil;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

// TODO move to inbot-utils
public class InitializingAtomicReference<V> {
    private V instance;
    private final Supplier<V> initializer;
    private final ReentrantLock lock = new ReentrantLock();

    public InitializingAtomicReference(Supplier<V> supplier) {
        this.initializer = supplier;
    }

    public V get() {
        if(instance==null) {
            lock.lock();
            if(instance == null) {
                instance = initializer.get();
            }
        }
        return instance;
    }
}
