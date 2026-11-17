package sqlancer.risingwave.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.AbstractAction;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.risingwave.RisingWaveErrors;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;

/** Schedules each budgeted insert once, mixed with current-schema DML and object creation. */
public final class RisingWaveSetupGenerator {
    private enum Kind {
        INSERT, UPDATE, DELETE, VIEW, MATERIALIZED_VIEW
    }

    private static final class Action implements AbstractAction<RisingWaveGlobalState> {
        private final Kind kind;
        private final String insert;
        private final String name;

        private Action(Kind kind, String insert, String name) {
            this.kind = kind;
            this.insert = insert;
            this.name = name;
        }

        @Override
        public SQLQueryAdapter getQuery(RisingWaveGlobalState state) throws Exception {
            if (kind == Kind.INSERT) {
                return new SQLQueryAdapter(insert);
            }
            List<RisingWaveTable> bases = state.getSchema().getDatabaseTables().stream()
                    .filter(RisingWaveTable::isWritable).collect(Collectors.toList());
            if (kind == Kind.VIEW || kind == Kind.MATERIALIZED_VIEW) {
                ExpectedErrors errors = RisingWaveErrors.expressionErrors();
                if (kind == Kind.MATERIALIZED_VIEW) {
                    errors.add("Not supported: streaming nested-loop join");
                }
                return new SQLQueryAdapter(
                        RisingWaveViewGenerator.create(state, bases, name, kind == Kind.MATERIALIZED_VIEW), errors,
                        true);
            }
            RisingWaveTable target = Randomly.fromList(bases);
            return new SQLQueryAdapter(kind == Kind.UPDATE ? RisingWaveUpdateGenerator.create(state, target)
                    : RisingWaveDeleteGenerator.create(state, target), RisingWaveErrors.expressionErrors());
        }

        @Override
        public boolean canBeRetried() {
            return false;
        }
    }

    private RisingWaveSetupGenerator() {
    }

    private static List<Action> actions(RisingWaveGlobalState state, List<RisingWaveTable> tables) {
        List<Action> actions = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++) {
            for (String insert : RisingWaveInsertGenerator.generate(state.getRandomly(), state.getDbmsSpecificOptions(),
                    tables.get(i), i)) {
                actions.add(new Action(Kind.INSERT, insert, null));
            }
        }
        for (Kind kind : List.of(Kind.VIEW, Kind.MATERIALIZED_VIEW)) {
            int max = kind == Kind.VIEW ? state.getDbmsSpecificOptions().maxViews
                    : state.getDbmsSpecificOptions().maxMaterializedViews;
            int count = state.getRandomly().getInteger(0, max + 1);
            for (int i = 0; i < count; i++) {
                String name = (kind == Kind.VIEW ? "v" : "mv") + i;
                actions.add(new Action(kind, null, name));
            }
        }
        int mutations = state.getRandomly().getInteger(0, state.getDbmsSpecificOptions().maxMutations + 1);
        for (int i = 0; i < mutations; i++) {
            actions.add(new Action(Randomly.getBoolean() ? Kind.UPDATE : Kind.DELETE, null, null));
        }
        return actions;
    }

    public static void execute(RisingWaveGlobalState state, List<RisingWaveTable> tables) throws Exception {
        List<Action> actions = actions(state, tables);
        new StatementExecutor<>(state, actions.toArray(new Action[0]), (s, action) -> 1, query -> {
        }).executeStatements();
    }
}
