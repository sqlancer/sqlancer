package sqlancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Test {
	
	
	public static void main(String[] args) {
		System.out.println(solution(new int[]{-1, -3}));
	}
	

	public static int solution(int[] a) {
		List<Integer> ints = new ArrayList<>(a.length);
		for (int i = 0; i < a.length; i++) {
			if (a[i] > 0) {
				ints.add(a[i]);
			}
		}
		Collections.sort(ints);
		if (ints.isEmpty()) {
			return 1;
		} else {
			int lastVal = 0;
			for (int i = 1; i < ints.size(); i++) {
				if (lastVal + 1 != ints.get(i) && lastVal != ints.get(i)) {
					return lastVal + 1;
				} else {
					lastVal = ints.get(i);
				}
			}
			return lastVal + 1;
		}
	}
	
}
