package sqlancer.oxla.schema;

import sqlancer.common.schema.AbstractTableColumn;

public class OxlaColumn extends AbstractTableColumn<OxlaTable, OxlaDataType> {
    public OxlaColumn(String name, OxlaDataType type) {
        super(name, null, type);
    }
}
