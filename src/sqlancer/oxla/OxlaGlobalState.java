package sqlancer.oxla;

import sqlancer.SQLGlobalState;
import sqlancer.oxla.schema.OxlaSchema;

import java.util.HashMap;
import java.util.Map;

public class OxlaGlobalState extends SQLGlobalState<OxlaOptions, OxlaSchema> {
    public final Map<String, Character> functionAndTypes = new HashMap<>();

    @Override
    protected OxlaSchema readSchema() throws Exception {
        return OxlaSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
