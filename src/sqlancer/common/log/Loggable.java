package sqlancer.common.log;

import java.io.Serializable;

public interface Loggable extends Serializable {
    String getLogString();
}
