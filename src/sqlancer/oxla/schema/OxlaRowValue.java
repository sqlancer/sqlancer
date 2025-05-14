package sqlancer.oxla.schema;

import sqlancer.common.schema.AbstractRowValue;
import sqlancer.oxla.ast.OxlaConstant;

import java.util.Map;

public class OxlaRowValue extends AbstractRowValue<OxlaTables, OxlaColumn, OxlaConstant>  {
    protected OxlaRowValue(OxlaTables tables, Map<OxlaColumn, OxlaConstant> values) {
        super(tables, values);
    }
}
