package sqlancer.tidb;

// do not make the fields final to avoid warnings
public class TiDBBugs {

	// https://github.com/pingcap/tidb/issues/15987
	public static boolean bug15987 = true;

	// // https://github.com/pingcap/tidb/issues/15988
	public static boolean bug15988 = true;

	// https://github.com/pingcap/tidb/issues/16028
	public static boolean bug16028 = true;

	// https://github.com/pingcap/tidb/issues/16020
	public static boolean bug16020 = true;

	// https://github.com/pingcap/tidb/issues/15990
	public static boolean bug15990 = true;

}
