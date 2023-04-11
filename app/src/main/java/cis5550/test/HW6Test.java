package cis5550.test;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

public class HW6Test extends GenericTest {

  public static void main(String[] args) throws Exception {

    /* Make a set of enabled tests. If no command-line arguments were specified, run all tests. */

    Set<String> tests = new TreeSet<>();
    boolean runSetup = true, runTests = true, promptUser = true, outputToFile = false, exitUponFailure = true, cleanup = true;

    if ((args.length > 0) && args[0].equals("auto")) {
      runSetup = false;
      runTests = true;
      outputToFile = true;
      exitUponFailure = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && args[0].equals("setup1")) {
      runSetup = true;
      runTests = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && args[0].equals("cleanup")) {
      runSetup = false;
      runTests = false;
      promptUser = false;
      cleanup = true;
    } else if ((args.length > 0) && args[0].equals("version")) {
      System.out.println("HW6 autograder v1.0 (Feb 19, 2023)");
      System.exit(1);
    }

    if ((args.length == 0) || args[0].equals("all") || args[0].equals("auto")) {
      tests.add("output");
      tests.add("collect");
      tests.add("flatmap");
      tests.add("maptopair");
      tests.add("foldbykey");
    }

    for (String arg : args) {
      if (!arg.equals("all") && !arg.equals("auto") && !arg.equals("setup")
          && !arg.equals("cleanup")) {
        tests.add(arg);
      }
    }

    HW6Test t = new HW6Test();
    t.setExitUponFailure(exitUponFailure);
    if (outputToFile) {
      t.outputToFile();
    }
    if (runSetup) {
      t.runSetup();
    }
    if (promptUser) {
      t.prompt();
    }
    if (runTests) {
      t.runTests(tests);
    }
    if (cleanup) {
      t.cleanup();
    }
  }

  void runSetup() {
  }

  void prompt() {
    System.out.println("In separate terminal windows, please run the following commands:");
    System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Master 8000");
    System.out.println(
        "  java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8001 worker1 localhost:8000");
    System.out.println(
        "  java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8002 worker2 localhost:8000");
    System.out.println(
        "  java -cp lib/kvs.jar:lib/webserver.jar cis5550.flame.Master 9000 localhost:8000");
    System.out.println(
        "  java -cp lib/kvs.jar:lib/webserver.jar cis5550.flame.Worker 9001 localhost:9000");
    System.out.println(
        "  java -cp lib/kvs.jar:lib/webserver.jar cis5550.flame.Worker 9002 localhost:9000");
    System.out.println("... and then hit Enter in this window to continue.");
    (new Scanner(System.in)).nextLine();
  }

  void cleanup() {
  }

  void runTests(Set<String> tests) throws Exception {

    /* Ask the user to confirm that the server is running */

    System.out.printf("\n%-10s%-40sResult\n", "Test", "Description");
    System.out.println("--------------------------------------------------------");

    KVSClient kvs = new KVSClient("localhost:8000");

    if (tests.contains("output")) {
      try {
        startTest("output", "Context.output()", 5);
        try {
          int num = 3 + (new Random()).nextInt(5);
          String[] arg = new String[num];
          StringBuilder expected = new StringBuilder("Worked, and the arguments are: ");
          for (int i = 0; i < num; i++) {
            arg[i] = randomAlphaNum(5, 10);
            expected.append((i > 0) ? "," : "").append(arg[i]);
          }
          String response = FlameSubmit.submit("localhost:9000", "tests/flame-output.jar",
              "cis5550.test.FlameOutput", arg);
          if (response == null) {
            testFailed(
                "We submitted a job (tests/flame-output.jar) to the Flame master, but it looks like the job failed. Here is the error output we got:\n\n"
                    + FlameSubmit.getErrorResponse());
          }
          if (Objects.equals(response, expected.toString())) {
            testSucceeded();
          } else {
            testFailed(
                "We expected to get '" + expected + "', but we actually got the following\n" + dump(
                    response.getBytes()));
          }
        } catch (ConnectException ce) {
          testFailed(
              "We were not able to connect to the Flame master at localhost:9000. Verify that the master is running and hasn't crashed?");
        }
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }
    }

    if (tests.contains("collect")) {
      try {
        startTest("collect", "RDD.collect()", 15);
        try {
          Random r = new Random();
          int num = 20 + r.nextInt(30), extra = 10;
          String[] arg = new String[num + extra];
          for (int i = 0; i < num; i++) {
            arg[i] = randomAlphaNum(5, 10);
          }
          for (int i = 0; i < extra; i++) {
            arg[num + i] = arg[r.nextInt(num)];
          }

          LinkedList<String> x = new LinkedList<>(Arrays.asList(arg).subList(0, (num + extra)));
          Collections.sort(x);
          StringBuilder expected = new StringBuilder();
          for (String s : x) {
            expected.append(expected.toString().equals("") ? "" : ",").append(s);
          }

          String response = FlameSubmit.submit("localhost:9000", "tests/flame-collect.jar",
              "cis5550.test.FlameCollect", arg);
          if (response == null) {
            testFailed(
                "We submitted a job (tests/flame-collect.jar) to the Flame master, but it looks like the job failed. Here is the error output we got:\n\n"
                    + FlameSubmit.getErrorResponse());
          }
          if (Objects.equals(response, expected.toString())) {
            testSucceeded();
          } else {
            testFailed(
                "We expected to get '" + expected + "', but we actually got the following\n" + dump(
                    response.getBytes()));
          }
        } catch (ConnectException ce) {
          testFailed(
              "We were not able to connect to the Flame master at localhost:9000. Verify that the master is running and hasn't crashed?");
        }
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }
    }

    if (tests.contains("flatmap")) {
      try {
        startTest("flatmap", "RDD.flatMap()", 25);
        try {
          String[] words = new String[]{"apple", "banana", "coconut", "date", "elderberry", "fig",
              "guava"};
          LinkedList<String> theWords = new LinkedList<>();
          Random r = new Random();
          int num = 5 + r.nextInt(10);
          String[] arg = new String[num];
          for (int i = 0; i < num; i++) {
            int nWords = 1 + r.nextInt(6);
            arg[i] = "";
            for (int j = 0; j < nWords; j++) {
              String w = words[r.nextInt(words.length)];
              arg[i] = arg[i] + (arg[i].equals("") ? "" : " ") + w;
              theWords.add(w);
            }
          }

          Collections.sort(theWords);

          StringBuilder argsAsString = new StringBuilder("(");
          for (int i = 0; i < arg.length; i++) {
            argsAsString.append((i > 0) ? "," : "").append("'").append(arg[i]).append("'");
          }
          argsAsString.append(")");

          StringBuilder expected = new StringBuilder();
          for (String s : theWords) {
            expected.append(expected.toString().equals("") ? "" : ",").append(s);
          }

          String response = FlameSubmit.submit("localhost:9000", "tests/flame-flatmap.jar",
              "cis5550.test.FlameFlatMap", arg);
          if (response == null) {
            testFailed(
                "We submitted a job (tests/flame-flatmap.jar) to the Flame master, but it looks like the job failed. Here is the error output we got:\n\n"
                    + FlameSubmit.getErrorResponse());
          }
          if (Objects.equals(response, expected.toString())) {
            testSucceeded();
          } else {
            testFailed("We sent " + argsAsString + " and expected to get '" + expected
                + "', but we actually got the following:\n\n" + dump(response.getBytes()));
          }
        } catch (ConnectException ce) {
          testFailed(
              "We were not able to connect to the Flame master at localhost:9000. Verify that the master is running and hasn't crashed?");
        }
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }
    }

    if (tests.contains("maptopair")) {
      try {
        startTest("maptopair", "RDD.mapToPair()", 10);
        try {
          String[] words = new String[]{"apple", "acorn", "banana", "blueberry", "coconut",
              "cranberry", "chestnut"};
          Random r = new Random();
          int num = 10 + r.nextInt(5);
          String[] arg = new String[num];
          List<String> exp = new LinkedList<>();
          for (int i = 0; i < num; i++) {
            arg[i] = words[r.nextInt(words.length)];
            exp.add("(" + arg[i].charAt(0) + "," + arg[i].substring(1) + ")");
          }

          Collections.sort(exp);
          StringBuilder expected = new StringBuilder();
          for (String s : exp) {
            expected.append(expected.toString().equals("") ? "" : ",").append(s);
          }

          String response = FlameSubmit.submit("localhost:9000", "tests/flame-maptopair.jar",
              "cis5550.test.FlameMapToPair", arg);
          if (response == null) {
            testFailed(
                "We submitted a job (tests/flame-maptopair.jar) to the Flame master, but it looks like the job failed. Here is the error output we got:\n\n"
                    + FlameSubmit.getErrorResponse());
          }
          if (response.equals(expected.toString())) {
            testSucceeded();
          } else {
            testFailed(
                "We expected to get '" + expected + "', but we actually got the following\n" + dump(
                    response.getBytes()));
          }
        } catch (ConnectException ce) {
          testFailed(
              "We were not able to connect to the Flame master at localhost:9000. Verify that the master is running and hasn't crashed?");
        }
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }
    }

    if (tests.contains("foldbykey")) {
      try {
        startTest("foldbykey", "PairRDD.foldByKey()", 10);
        try {
          Random r = new Random();
          int num = 20 + r.nextInt(5);
          String[] arg = new String[num];
          String chr = "ABC";
          int[] sum = new int[chr.length()];
          for (int i = 0; i < chr.length(); i++) {
            sum[i] = r.nextInt(20);
            arg[i] = chr.charAt(i) + " " + sum[i];
          }
          for (int i = chr.length(); i < num; i++) {
            int v = r.nextInt(20);
            int which = r.nextInt(chr.length());
            sum[which] += v;
            arg[i] = chr.charAt(which) + " " + v;
          }

          StringBuilder argsAsString = new StringBuilder("(");
          for (int i = 0; i < arg.length; i++) {
            argsAsString.append((i > 0) ? "," : "").append("'").append(arg[i]).append("'");
          }
          argsAsString.append(")");

          StringBuilder expected = new StringBuilder();
          for (int i = 0; i < chr.length(); i++) {
            expected.append(expected.toString().equals("") ? "" : ",").append("(")
                .append(chr.charAt(i)).append(",").append(sum[i]).append(")");
          }

          String response = FlameSubmit.submit("localhost:9000", "tests/flame-foldbykey.jar",
              "cis5550.test.FlameFoldByKey", arg);
          if (response == null) {
            testFailed(
                "We submitted a job (tests/flame-foldbykey.jar) to the Flame master, but it looks like the job failed. Here is the error output we got:\n\n"
                    + FlameSubmit.getErrorResponse());
          }
          if (Objects.equals(response, expected.toString())) {
            testSucceeded();
          } else {
            testFailed("We sent " + argsAsString + " and expected to get '" + expected
                + "', but we actually got the following:\n\n" + dump(response.getBytes()));
          }
        } catch (ConnectException ce) {
          testFailed(
              "We were not able to connect to the Flame master at localhost:9000. Verify that the master is running and hasn't crashed?");
        }
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }
    }

    System.out.println("--------------------------------------------------------\n");
    if (numTestsFailed == 0) {
      System.out.println(
          "Looks like your solution passed all of the selected tests. Congratulations!");
    } else {
      System.out.println(numTestsFailed + " test(s) failed.");
    }

    cleanup();
    closeOutputFile();
  }
}
