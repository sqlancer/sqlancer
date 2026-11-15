package sqlancer.voltdb.oracle;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;

/**
 * GEOGRAPHY_POINT and GEOGRAPHY against geography computed in Java. On a database's first check, the oracle creates a
 * table {@code <prefix>GEO (id, p GEOGRAPHY_POINT, g GEOGRAPHY)}, replicated or partitioned on id, fills it with random
 * points and small counter-clockwise boxes (VoltDB rejects a clockwise ring as "not normalized"), and checks:
 * <ul>
 * <li>{@code LATITUDE(p)} and {@code LONGITUDE(p)} return the inserted coordinates, also after
 * {@code POINTFROMTEXT(ASTEXT(p))};</li>
 * <li>{@code DISTANCE(a.p, b.p)} for every pair is the great-circle distance on a sphere of radius 6,371,008.8 m,
 * within a relative 1e-9, and {@code WHERE DISTANCE(a.p, b.p) < r} returns the pairs closer than r;</li>
 * <li>{@code CONTAINS(b.g, a.p)} for every point and box is the containment computed in Java. A box's edges are great
 * circle arcs, not lines of latitude, so points are drawn away from every box's border (a fifth of its size); the check
 * runs before and after {@code CREATE INDEX} on g, which VoltDB uses for CONTAINS.</li>
 * </ul>
 * It runs on one database in four, as its catalog updates block the whole cluster. The table is dropped at the end. The
 * schema reader ignores it, so other oracles never see its columns. A failure has no reproducer; its message holds the
 * rows and the query.
 */
public class VoltDBGeographyOracle implements TestOracle<VoltDBGlobalState> {

    // VoltDB's earth: one degree of longitude at the equator is 111,195.0802335329 m
    static final double RADIUS = 111195.0802335329 * 180 / Math.PI;

    private static final int ROWS = 12;
    private static final int SAMPLE = 4;

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private boolean done;
    private String lastQuery;
    private final List<String> statements = new ArrayList<>();

    private static final class Box {
        final double lon;
        final double lat;
        final double width;
        final double height;

        Box(double lon, double lat, double width, double height) {
            this.lon = lon;
            this.lat = lat;
            this.width = width;
            this.height = height;
        }

        // 1 inside, 0 outside, -1 too close to the border to tell
        int contains(double plon, double plat) {
            double mx = width / 5;
            double my = height / 5;
            boolean inside = plon > lon + mx && plon < lon + width - mx && plat > lat + my && plat < lat + height - my;
            boolean outside = plon < lon - mx || plon > lon + width + mx || plat < lat - my || plat > lat + height + my;
            return inside ? 1 : outside ? 0 : -1;
        }

        String wkt() {
            double e = lon + width;
            double n = lat + height;
            return String.format(Locale.ROOT, "POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))", fmt(lon), fmt(lat),
                    fmt(e), fmt(lat), fmt(e), fmt(n), fmt(lon), fmt(n), fmt(lon), fmt(lat));
        }
    }

    public VoltDBGeographyOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    static String fmt(double d) {
        return String.format(Locale.ROOT, "%.6f", d);
    }

    // the great-circle distance in meters (haversine)
    static double distance(double lon1, double lat1, double lon2, double lat2) {
        double p1 = Math.toRadians(lat1);
        double p2 = Math.toRadians(lat2);
        double dp = p2 - p1;
        double dl = Math.toRadians(lon2 - lon1);
        double h = Math.sin(dp / 2) * Math.sin(dp / 2)
                + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
        return 2 * RADIUS * Math.asin(Math.min(1, Math.sqrt(h)));
    }

    @Override
    public void check() throws Exception {
        if (done) {
            throw new IgnoreMeException();
        }
        done = true;
        // its four catalog updates (CREATE TABLE, CREATE INDEX, two DROPs) block the whole cluster, which cost the
        // other oracles about 40% of their queries when GEO ran on every database; it runs on one database in SAMPLE
        if (Randomly.getNotCachedInteger(0, SAMPLE) != 0) {
            throw new IgnoreMeException();
        }
        statements.clear();
        Randomly r = state.getRandomly();
        String table = state.getPrefix() + "GEO";
        boolean partitioned = Randomly.getBoolean();
        List<Box> boxes = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            double w = 0.5 + r.getInteger(0, 450) / 100.0;
            double h = 0.5 + r.getInteger(0, 450) / 100.0;
            boxes.add(new Box(round(-170 + r.getInteger(0, 33_000) / 100.0),
                    round(-70 + r.getInteger(0, 13_500) / 100.0), w, h));
        }
        // half the points near a box, so that some are inside one; none near any box's border
        double[][] points = new double[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            for (int attempt = 0; attempt < 100 && points[i] == null; attempt++) {
                double lon;
                double lat;
                if (Randomly.getBoolean()) {
                    Box b = Randomly.fromList(boxes);
                    lon = round(b.lon + b.width * r.getInteger(-50, 150) / 100.0);
                    lat = round(b.lat + b.height * r.getInteger(-50, 150) / 100.0);
                } else {
                    lon = round(-179 + r.getInteger(0, 35_800) / 100.0);
                    lat = round(-80 + r.getInteger(0, 16_000) / 100.0);
                }
                final double plon = lon;
                final double plat = lat;
                if (boxes.stream().noneMatch(b -> b.contains(plon, plat) == -1)) {
                    points[i] = new double[] { lon, lat };
                }
            }
            if (points[i] == null) {
                throw new IgnoreMeException();
            }
        }

        try {
            ddl("DROP TABLE " + table + " IF EXISTS");
            ddl("CREATE TABLE " + table + " (id INTEGER NOT NULL, p GEOGRAPHY_POINT, g GEOGRAPHY(1024))"
                    + (partitioned ? "; PARTITION TABLE " + table + " ON COLUMN id" : ""));
            for (int i = 0; i < ROWS; i++) {
                execute("INSERT INTO " + table + " VALUES (" + i + ", POINTFROMTEXT('POINT(" + fmt(points[i][0]) + " "
                        + fmt(points[i][1]) + ")'), POLYGONFROMTEXT('" + boxes.get(i).wkt() + "'))");
            }
            checkPoints(table, points);
            checkDistances(table, points, partitioned);
            checkContains(table, points, boxes, partitioned, "");
            ddl("CREATE INDEX " + state.getFreeIndexName() + " ON " + table + " (g)");
            checkContains(table, points, boxes, partitioned, " (with the index on g)");
        } finally {
            try {
                ddl("DROP TABLE " + table + " IF EXISTS");
            } catch (IgnoreMeException | AssertionError e) {
                // dropped with the database's leftovers when its name is next used
            }
        }
    }

    private static double round(double d) {
        return Math.round(d * 1e6) / 1e6;
    }

    // VoltDB keeps a point as a unit vector, so its coordinates come back within 1e-9 degrees, not exactly
    private void checkPoints(String table, double[]... points) throws SQLException {
        String q = "SELECT id, LATITUDE(p), LONGITUDE(p), LATITUDE(POINTFROMTEXT(ASTEXT(p))), "
                + "LONGITUDE(POINTFROMTEXT(ASTEXT(p))) FROM " + table;
        lastQuery = q;
        int seen = 0;
        for (List<Object> row : VoltDBRows.values(state, q, errors, 5)) {
            int i = ((Number) row.get(0)).intValue();
            seen++;
            for (int c = 1; c <= 4; c++) {
                double expected = c % 2 == 1 ? points[i][1] : points[i][0];
                double actual = ((Number) row.get(c)).doubleValue();
                if (Math.abs(actual - expected) > 1e-9) {
                    fail(q, "row " + i + " column " + (c + 1) + ": " + actual + ", expected " + expected);
                }
            }
        }
        if (seen != points.length) {
            fail(q, "expected " + points.length + " rows, got " + seen);
        }
    }

    private void checkDistances(String table, double[][] points, boolean partitioned) throws SQLException {
        if (partitioned) {
            // a join of a partitioned table with itself needs an equality on the partitioning column
            return;
        }
        String q = "SELECT a.id, b.id, DISTANCE(a.p, b.p) FROM " + table + " a, " + table + " b";
        lastQuery = q;
        for (List<Object> row : VoltDBRows.values(state, q, errors, 3)) {
            int a = ((Number) row.get(0)).intValue();
            int b = ((Number) row.get(1)).intValue();
            double actual = ((Number) row.get(2)).doubleValue();
            double expected = distance(points[a][0], points[a][1], points[b][0], points[b][1]);
            if (Math.abs(actual - expected) > 1e-9 * Math.max(1, expected) + 1e-6) {
                fail(q, "DISTANCE of " + a + " and " + b + ": " + actual + ", expected " + expected);
            }
        }
        double limit = 1000 * Randomly.fromOptions(100, 500, 2000, 8000);
        String filtered = "SELECT a.id, b.id FROM " + table + " a, " + table + " b WHERE DISTANCE(a.p, b.p) < " + limit;
        List<String> expected = new ArrayList<>();
        for (int a = 0; a < points.length; a++) {
            for (int b = 0; b < points.length; b++) {
                double d = distance(points[a][0], points[a][1], points[b][0], points[b][1]);
                if (Math.abs(d - limit) < 1) {
                    return; // too close to the limit to tell
                }
                if (d < limit) {
                    expected.add(a + " | " + b);
                }
            }
        }
        compare(filtered, expected, rows(filtered, 2));
    }

    private void checkContains(String table, double[][] points, List<Box> boxes, boolean partitioned, String when)
            throws SQLException {
        List<String> expected = new ArrayList<>();
        String q;
        if (partitioned) {
            // each row's point against its own box: an equality on the partitioning column
            q = "SELECT id FROM " + table + " WHERE CONTAINS(g, p)";
            for (int i = 0; i < points.length; i++) {
                if (boxes.get(i).contains(points[i][0], points[i][1]) == 1) {
                    expected.add(Integer.toString(i));
                }
            }
            compare(q + when, expected, rows(q, 1));
        } else {
            q = "SELECT a.id, b.id FROM " + table + " a, " + table + " b WHERE CONTAINS(b.g, a.p)";
            for (int a = 0; a < points.length; a++) {
                for (int b = 0; b < boxes.size(); b++) {
                    if (boxes.get(b).contains(points[a][0], points[a][1]) == 1) {
                        expected.add(a + " | " + b);
                    }
                }
            }
            compare(q + when, expected, rows(q, 2));
        }
    }

    private List<String> rows(String q, int columns) throws SQLException {
        lastQuery = q;
        List<String> rows = VoltDBRows.strings(state, q, errors, columns);
        Collections.sort(rows);
        return rows;
    }

    private void compare(String q, List<String> expected, List<String> actual) {
        Collections.sort(expected);
        if (!expected.equals(actual)) {
            fail(q, "expected " + expected.size() + " rows, got " + actual.size() + System.lineSeparator()
                    + "-- missing: " + VoltDBRows.difference(expected, actual) + System.lineSeparator()
                    + "-- unexpected: " + VoltDBRows.difference(actual, expected));
        }
    }

    private void fail(String q, String problem) {
        throw new AssertionError("geography mismatch:" + System.lineSeparator()
                + String.join(";" + System.lineSeparator(), statements) + ";" + System.lineSeparator() + "-- query: "
                + q + ";" + System.lineSeparator() + "-- " + problem);
    }

    private void ddl(String sql) {
        execute(sql);
    }

    // a statement the check depends on: any error but a node failure (IgnoreMeException) is a failure
    private void execute(String sql) {
        statements.add(sql);
        try (Statement st = state.getConnection().createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            throw new AssertionError(sql, e);
        }
    }

    @Override
    public Reproducer<VoltDBGlobalState> getLastReproducer() {
        return null;
    }

    @Override
    public String getLastQueryString() {
        return lastQuery;
    }

}
