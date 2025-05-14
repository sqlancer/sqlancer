package sqlancer.oxla.ast;

@FunctionalInterface
public interface OxlaApplyFunction {
    OxlaConstant apply(OxlaConstant[] constants);
}
