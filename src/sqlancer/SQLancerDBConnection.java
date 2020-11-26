package sqlancer;

public interface SQLancerDBConnection extends AutoCloseable {

    String getDatabaseVersion() throws Exception;
}
