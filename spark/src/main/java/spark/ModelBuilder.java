package spark;

import scala.Serializable;

/**
 * Provides an interface for all model builder concrete classes.
 */
public interface ModelBuilder<T> extends Serializable {

    public T build();

}
