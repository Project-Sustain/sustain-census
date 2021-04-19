package spark;

import scala.Serializable;

public class SerializableModel implements Serializable {

    public int i;

    public SerializableModel(int i) {
        this.i = i;
    }

}
