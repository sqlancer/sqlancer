package sqlancer.simple.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.simple.Dialect;
import sqlancer.simple.Expression;
import sqlancer.simple.Signal;
import sqlancer.simple.SignalResponse;
import sqlancer.simple.expression.ColumnName;
import sqlancer.simple.expression.Constant;
import sqlancer.simple.expression.Operation;
import sqlancer.simple.type.Type;

public class SelectGenerator {

    List<AbstractTable<?, ?, ?>> tables;
    Dialect dialect;

    Expression generateExpression() {
        List<Operation> ops = dialect.getOperations();
        assert !ops.isEmpty() : "generateExpression(): dialect does not have any operations";

        Operation op = Randomly.fromList(ops);

        List<SignalResponse> signalResponses = new ArrayList<>();
        for (Signal signal : op.getRequestSignals()) {
            SignalResponse response;
            switch (signal) {
            case COLUMN_NAME:
                response = new SignalResponse.ExpressionResponse(generateColumn());
                break;
            case CONSTANT_VALUE:
                response = new SignalResponse.ExpressionResponse(generateConstant());
                break;
            case TYPE:
                response = new SignalResponse.StringResponse(getRandomType().toString());
                break;
            case EXPRESSION:
                response = new SignalResponse.ExpressionResponse(generateExpression());
                break;

            default:
                throw new AssertionError(signal + " not handled in generateExpression()");
            }
            signalResponses.add(response);
        }

        return op.create(signalResponses);
    }

    Type getRandomType() {
        List<Type> types = dialect.getTypes();
        assert !types.isEmpty() : "generateConstant(): dialect does not have any types";

        return Randomly.fromList(types);
    }

    Expression generateColumn() {
        assert !tables.isEmpty() : "generateColumn(): generator's table list is empty";

        AbstractTable<?, ?, ?> table = Randomly.fromList(tables);
        AbstractTableColumn<?, ?> column = Randomly.fromList(table.getColumns());

        return new ColumnName(column.getName(), table.getName());
    }

    Expression generateConstant() {
        Type type = getRandomType();
        String value = type.instantiate();

        return new Constant(value);
    }
}
