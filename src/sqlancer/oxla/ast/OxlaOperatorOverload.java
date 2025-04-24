package sqlancer.oxla.ast;

import sqlancer.oxla.schema.OxlaDataType;

public class OxlaOperatorOverload {
    public final OxlaDataType[] inputTypes;
    public final OxlaDataType returnType;

    public OxlaOperatorOverload(OxlaDataType[] inputTypes, OxlaDataType returnType) {
        this.inputTypes = inputTypes;
        this.returnType = returnType;
    }

    public OxlaOperatorOverload(OxlaDataType inputType, OxlaDataType returnType) {
        this.inputTypes = new OxlaDataType[]{inputType};
        this.returnType = returnType;
    }
}
