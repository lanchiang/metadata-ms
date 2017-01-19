package de.hpi.isg.mdms.model.common;

/**
 * Own slim implementation of the <a href="http://en.wikipedia.org/wiki/Observer_pattern">Observer pattern</a>. Observer
 * get notified by {@link java.util.Observable}s.
 */
public interface Observer<T> {
    /**
     * Generates random integer ids.
     * 
     * @return
     */
    int generateRandomId();
    int getUnusedSchemaId();

    /**
     * This method is called by objects that want to be registered by the {@link Observer}.
     * 
     * @param t
     *        to register
     */
    void registerTargetObject(T t);
}
