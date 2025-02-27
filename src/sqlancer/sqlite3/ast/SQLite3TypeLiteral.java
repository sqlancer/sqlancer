package sqlancer.sqlite3.ast;

public class SQLite3TypeLiteral {

    public final Type type;

    public enum Type {
        TEXT {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToText(cons);
            }
        },
        REAL {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToReal(cons);
            }
        },
        INTEGER {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToInt(cons);
            }
        },
        NUMERIC {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToNumeric(cons);
            }
        },
        BLOB {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToBlob(cons);
            }
        };

        public abstract SQLite3Constant apply(SQLite3Constant cons);
    }

    public SQLite3TypeLiteral(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }
}
