package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FlameFoldByKey {

  public static void run(FlameContext ctx, String[] args) throws Exception {
    LinkedList<String> list = new LinkedList<String>();
		Collections.addAll(list, args);

    List<FlamePair> out = ctx.parallelize(list)
        .mapToPair(s -> {
          String[] pieces = s.split(" ");
          return new FlamePair(pieces[0], pieces[1]);
        })
        .foldByKey("0", (a, b) -> "" + (Integer.valueOf(a) + Integer.valueOf(b)))
        .collect();

    Collections.sort(out);

    String result = "";
		for (FlamePair p : out) {
			result = result + (result.equals("") ? "" : ",") + p.toString();
		}

    ctx.output(result);
  }
}