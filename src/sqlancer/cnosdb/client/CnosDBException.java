package sqlancer.cnosdb.client;

public class CnosDBException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    CnosDBException(String message) {
        super(message);
    }
}
