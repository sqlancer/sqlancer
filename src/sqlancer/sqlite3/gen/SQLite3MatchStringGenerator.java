package sqlancer.sqlite3.gen;

import sqlancer.Randomly;

public class SQLite3MatchStringGenerator {

    private final Randomly r;
    private int depth;
    private final StringBuilder sb = new StringBuilder();

    public SQLite3MatchStringGenerator(Randomly r) {
        this.r = r;
        depth = r.getInteger(0, 10);
    }

    public static String generateMatchString(Randomly r) {
        SQLite3MatchStringGenerator gen = new SQLite3MatchStringGenerator(r);
        gen.generate();
        return gen.sb.toString();
    }

    enum MatchAction {
        NEAR, LOGICAL_BINARY_OPERATOR, NOT, COLUMN, COLSPEC, STRING
    };

    private void generate() {
        if (depth == 0) {
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                if (i != 0) {
                    sb.append(" ");
                }
                sb.append(r.getString());
            }
        } else {
            depth--;
            MatchAction action = Randomly.fromOptions(MatchAction.values());
            switch (action) {
            case STRING:
                sb.append(r.getString());
                sb.append(" ");
                break;
            case COLSPEC:
                appendColspec();
                break;
            case COLUMN:
                sb.append("c");
                sb.append(Randomly.smallNumber());
                break;
            case NEAR:
                generate();
                if (Randomly.getBoolean()) {
                    sb.append(" NEAR ");
                } else {
                    sb.append(String.format("NEAR/%d", r.getInteger()));
                }
                generate();
                break;
            case LOGICAL_BINARY_OPERATOR:
                generate();
                sb.append(" ");
                sb.append(Randomly.fromOptions("AND", "OR", "+"));
                sb.append(" ");
                generate();
                break;
            case NOT:
                sb.append(Randomly.fromOptions("-", "NOT"));
                sb.append(" ");
                generate();
                break;
            default:
                throw new AssertionError();
            }
        }
    }

    private void appendColspec() {
        boolean braces = Randomly.getBoolean();
        if (braces) {
            sb.append("{");
        }
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("c");
            sb.append(Randomly.smallNumber());
        }
        if (braces) {
            sb.append("}");
        }
    }

}
