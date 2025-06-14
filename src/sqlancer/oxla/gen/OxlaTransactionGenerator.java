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

public class OxlaTransactionGenerator extends OxlaQueryGenerator {
    enum TransactionMode {
        READ_WRITE,
        READ_ONLY,
        DEFERRABLE,
        NOT_DEFERRABLE,
        ISOLATION_LEVEL_READ_UNCOMMITED,
        ISOLATION_LEVEL_READ_COMMITED,
        ISOLATION_LEVEL_REPEATABLE_READ,
        ISOLATION_LEVEL_SERIALIZABLE;

        public static TransactionMode getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    private static final List<String> errors = List.of(
            "READ ONLY transactions are not supported"
    );
    private static final List<Pattern> regexErrors = List.of(
            Pattern.compile("\\S+ options in transactions \\S+ not supported")
    );
    private static final ExpectedErrors expectedErrors = new ExpectedErrors(errors, regexErrors)
            .addAll(OxlaCommon.ALL_ERRORS);

    @Override
    public SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth) {
        // isolation_level       := READ UNCOMMITED | READ COMMITED | REPEATABLE READ | SERIALIZABLE
        // transaction_mode      := READ WRITE | READ ONLY | DEFERRABLE | NOT DEFERRABLE | ISOLATION LEVEL isolation_level
        // transaction_keyword   := TRANSACTION | WORK
        // transaction_statement := BEGIN [transaction_keyword] [transaction_mode [[,] ...]
        //                        | START TRANSACTION [transaction_mode [[,] ...]
        //                        | ROLLBACK [transaction_keyword]
        //                        | COMMIT [transaction_keyword]
        //                        | END [transaction_keyword]
        enum Rule {BEGIN_TRANSACTION, START_TRANSACTION, ROLLBACK_TRANSACTION, COMMIT_TRANSACTION, END_TRANSACTION}
        final var keyword = Randomly.getBoolean() ? getTransactionKeyword() : "";
        final var modeList = Randomly.getBoolean() ? asTransactionModeList() : "";
        final var queryBuilder = new StringBuilder();
        switch (Randomly.fromOptions(Rule.values())) {
            case BEGIN_TRANSACTION -> queryBuilder.append("BEGIN ").append(keyword).append(' ').append(modeList);
            case START_TRANSACTION -> queryBuilder.append("START TRANSACTION ").append(modeList);
            case ROLLBACK_TRANSACTION -> queryBuilder.append("ROLLBACK ").append(keyword);
            case COMMIT_TRANSACTION -> queryBuilder.append("COMMIT ").append(keyword);
            case END_TRANSACTION -> queryBuilder.append("END ").append(keyword);
        }

        return new SQLQueryAdapter(queryBuilder.toString(), expectedErrors);
    }

    private String asTransactionMode(TransactionMode mode) {
        return switch (mode) {
            case READ_WRITE -> "READ WRITE";
            case READ_ONLY -> "READ ONLY";
            case DEFERRABLE -> "DEFERRABLE";
            case NOT_DEFERRABLE -> "NOT DEFERRABLE";
            case ISOLATION_LEVEL_READ_UNCOMMITED -> "ISOLATION LEVEL READ UNCOMMITED";
            case ISOLATION_LEVEL_READ_COMMITED -> "ISOLATION LEVEL READ COMMITED";
            case ISOLATION_LEVEL_REPEATABLE_READ -> "ISOLATION LEVEL REPEATABLE READ";
            case ISOLATION_LEVEL_SERIALIZABLE -> "ISOLATION LEVEL SERIALIZABLE";
        };
    }

    private String asTransactionModeList() {
        return Randomly.nonEmptySubsetPotentialDuplicates(Arrays.asList(TransactionMode.values()))
                .stream()
                .map(this::asTransactionMode)
                .collect(Collectors.joining(Randomly.getBoolean() ? " " : ", "));
    }

    private String getTransactionKeyword() {
        enum Keyword {NONE, TRANSACTION, WORK}
        return switch (Randomly.fromOptions(Keyword.values())) {
            case NONE -> "";
            case TRANSACTION -> "TRANSACTION";
            case WORK -> "WORK";
        };
    }
}
