package sqlancer.simple.expression;

import java.util.List;

import sqlancer.simple.Expression;
import sqlancer.simple.Signal;
import sqlancer.simple.SignalResponse;

public interface Operation {
    List<Signal> getRequestSignals();

    Expression create(List<SignalResponse> innerExpressions);
}
