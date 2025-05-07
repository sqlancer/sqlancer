package sqlancer.oxla.ast;

import sqlancer.oxla.schema.OxlaDataType;

public class OxlaTypeOverload {
    public final OxlaDataType[] inputTypes;
    public final OxlaDataType returnType;

    public OxlaTypeOverload(OxlaDataType[] inputTypes, OxlaDataType returnType) {
        this.inputTypes = inputTypes;
        this.returnType = returnType;
    }

    public OxlaTypeOverload(OxlaDataType inputType, OxlaDataType returnType) {
        this.inputTypes = new OxlaDataType[]{inputType};
        this.returnType = returnType;
    }
}
