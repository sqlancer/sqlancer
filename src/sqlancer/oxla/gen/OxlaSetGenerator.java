package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaCommon;
import sqlancer.oxla.OxlaGlobalState;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OxlaSetGenerator extends OxlaQueryGenerator {
    enum ValueType {PLAIN, STRING, INT_LITERAL, FLOAT_LITERAL, ON_KEYWORD}

    private static final List<String> errors = List.of();
    private static final List<Pattern> regexErrors = List.of();
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // set_value     := database_object_name | string | int_literal | float_literal | ON
        // set_statement := SET [SESSION | LOCAL] database_object_name ['.' database_object_name] ('=' | TO) set_value [, ...]
        final var randomly = globalState.getRandomly();
        final var queryBuilder = new StringBuilder()
                .append("SET ")
                .append(Randomly.getBoolean() ? (Randomly.getBoolean() ? "SESSION " : "LOCAL ") : "")
                .append(randomly.getString())
                .append(' ')
                .append(Randomly.getBoolean() ? String.format("DOT %s ", randomly.getString()) : "")
                .append(Randomly.getBoolean() ? "TO " : "= ")
                .append(Randomly.nonEmptySubsetPotentialDuplicates(Arrays.asList(ValueType.values()))
                        .stream()
                        .map(type -> value(globalState, type))
                        .collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private String value(OxlaGlobalState globalState, ValueType type) {
        final var randomly = globalState.getRandomly();
        return switch (type) {
            case PLAIN -> randomly.getString();
            case STRING -> String.format("'%s'", randomly.getString());
            case INT_LITERAL -> String.valueOf(randomly.getInteger());
            case FLOAT_LITERAL -> String.valueOf(randomly.getFloat());
            case ON_KEYWORD -> "ON";
        };
    }
}
