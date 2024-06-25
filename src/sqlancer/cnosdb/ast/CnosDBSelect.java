package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.SelectBase;

public class CnosDBSelect extends SelectBase<CnosDBExpression> implements CnosDBExpression {

    private SelectType selectType = SelectType.ALL;

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public void setSelectType(SelectType selectType) {
        this.selectType = selectType;
    }

    public SelectType getSelectType() {
        return selectType;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return null;
    }

    // public static class CnosDBSubquery implements CnosDBExpression {
    //     private final CnosDBSelect s;
    //     private final String name;

    //     public CnosDBSubquery(CnosDBSelect s, String name) {
    //         this.s = s;
    //         this.name = name;
    //     }

    //     public CnosDBSelect getSelect() {
    //         return s;
    //     }

    //     public String getName() {
    //         return name;
    //     }

    //     @Override
    //     public CnosDBDataType getExpressionType() {
    //         return null;
    //     }
    // }

}
