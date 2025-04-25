package sqlancer.oxla.schema;

import sqlancer.common.schema.AbstractTables;

import java.util.List;

public class OxlaTables extends AbstractTables<OxlaTable, OxlaColumn> {
    public OxlaTables(List<OxlaTable> tables) {
        super(tables);
    }
}
