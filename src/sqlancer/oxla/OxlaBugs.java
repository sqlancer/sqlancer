package sqlancer.oxla;

public final class OxlaBugs {
    private OxlaBugs() {
    }

    /// See: https://oxla.atlassian.net/browse/OXLA-3376
    /// Valid INT_MIN value results in an "Integer literal error. Value of literal exceeds range." parsing error.
    public static boolean bugOxla3376 = true;

    /// See: https://oxla.atlassian.net/browse/OXLA-8323
    /// Errors caused in JOIN's WHERE condition return internal error non-deterministically.
    public static boolean bugOxla8323 = true;

    /// See: https://oxla.atlassian.net/browse/OXLA-8328
    /// Adding/Subtracting large integers to/from a date will crash Oxla in Debug builds, and fail silently in Release.
    public static boolean bugOxla8328 = true;

    /// See: https://oxla.atlassian.net/browse/OXLA-8329
    /// Oxla instantly crashes for REGEX patterns containing invalid symbols.
    public static boolean bugOxla8329 = true;

    /// See: https://oxla.atlassian.net/browse/OXLA-8330
    /// Oxla parses ~~, !~~, ~~*, !~~* operators incorrectly.
    public static boolean bugOxla8330 = true;

    /// See: https://oxla.atlassian.net/browse/OXLA-8332
    /// Oxla returns Internal Compiler Error for NULL literal JSON extract(s).
    public static boolean bugOxla8332 = true;

    /// See: https://oxla.atlassian.net/browse/OXLA-8347
    /// PG_TYPEOF function resolves its type into the expression's type instead of text.
    public static boolean bugOxla8347 = true;

    /// See: https://oxla.atlassian.net/browse/OXLA-8349
    /// Some FOR_MIN/FOR_MAX queries cause the Oxla to crash.
    public static boolean bugOxla8349 = true;

    /// See: https://oxla.atlassian.net/browse/OXLA-8350
    /// 'pg_*' functions that accept INT4 do not work with INT8.
    public static boolean bugOxla8350 = true;
}
