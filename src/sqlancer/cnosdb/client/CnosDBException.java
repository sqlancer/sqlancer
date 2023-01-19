package sqlancer.cnosdb.client;

public class CnosDBException extends RuntimeException {
    CnosDBException(String message) {
        super(message);
    }
}
