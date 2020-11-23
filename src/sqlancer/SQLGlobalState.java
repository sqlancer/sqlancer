package sqlancer;

import sqlancer.common.schema.AbstractSchema;

/**
 * Represents a global state that is valid for a testing session on a given database.
 *
 * @param <O>
 *            the option parameter
 * @param <S>
 *            the schema parameter
 */
public abstract class SQLGlobalState<O extends DBMSSpecificOptions<?>, S extends AbstractSchema<?, ?>>
    extends GlobalState<O, S, SQLConnection> {
}
