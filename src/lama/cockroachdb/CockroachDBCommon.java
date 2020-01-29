package lama.cockroachdb;

import lama.Randomly;

public class CockroachDBCommon {
	
	public static String getRandomCollate() {
		return Randomly.fromOptions("en", "de", "es", "cmn");
	}

}
