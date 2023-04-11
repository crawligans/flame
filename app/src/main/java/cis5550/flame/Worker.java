package cis5550.flame;

import static cis5550.webserver.Server.port;
import static cis5550.webserver.Server.post;

import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlameContextImpl.Operation;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.naming.OperationNotSupportedException;

class Worker extends cis5550.generic.Worker {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Syntax: Worker <port> <masterIP:port>");
      System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
    startPingThread(server, "" + port, port);
    final File myJAR = new File("__worker" + port + "-current.jar");

    port(port);

    post("/useJAR", (request, response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    post(Operation.FLAT_MAP.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      Object lambda;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof StringToIterable || opArgs[0] instanceof PairToStringIterable;
        lambda = opArgs[0];
      } else {
        assert parsedArgs instanceof StringToIterable || parsedArgs instanceof PairToStringIterable;
        lambda = parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };

      if (lambda instanceof StringToIterable) {
        FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
        context.parallelize(req.queryParams("outputTable"),
            flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
                .map(a -> {
                  try {
                    return ((StringToIterable) lambda).op(a);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }).filter(Objects::nonNull)
                .flatMap(i -> StreamSupport.stream(i.spliterator(), true)));
      } else {
        FlamePairRDDImpl flameRDD = new FlamePairRDDImpl(context, req.queryParams("inputTable"));
        context.parallelize(req.queryParams("outputTable"),
            flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
                .map(a -> {
                  try {
                    return ((PairToStringIterable) lambda).op(a);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }).filter(Objects::nonNull)
                .flatMap(i -> StreamSupport.stream(i.spliterator(), true)));
      }
      return output.toString();
    });

    post(Operation.FLAT_MAP_TO_PAIR.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      Object lambda;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof StringToPairIterable || opArgs[0] instanceof PairToPairIterable;
        lambda = opArgs[0];
      } else {
        assert
            parsedArgs instanceof StringToPairIterable || parsedArgs instanceof PairToPairIterable;
        lambda = parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };

      if (lambda instanceof StringToPairIterable) {
        FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
        context.parallelizePairs(req.queryParams("outputTable"),
            flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
                .map(a -> {
                  try {
                    return ((StringToPairIterable) lambda).op(a);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }).filter(Objects::nonNull)
                .flatMap(i -> StreamSupport.stream(i.spliterator(), true)));
      } else {
        FlamePairRDDImpl flameRDD = new FlamePairRDDImpl(context, req.queryParams("inputTable"));
        context.parallelizePairs(req.queryParams("outputTable"),
            flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
                .map(a -> {
                  try {
                    return ((PairToPairIterable) lambda).op(a);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }).filter(Objects::nonNull)
                .flatMap(i -> StreamSupport.stream(i.spliterator(), true)));
      }
      return output.toString();
    });

    post(Operation.MAP_TO_PAIR.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      StringToPair lambda;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof StringToPair;
        lambda = (StringToPair) opArgs[0];
      } else {
        assert parsedArgs instanceof StringToPair;
        lambda = (StringToPair) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
      context.parallelizePairs(req.queryParams("outputTable"),
          flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive")).map(a -> {
            try {
              return lambda.op(a);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }));
      return output.toString();
    });

    post(Operation.FOLD.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      String zeroElement;
      TwoStringsToString lambda;
      if (parsedArgs instanceof Object[] opArgs) {
        if (opArgs.length >= 2 && opArgs[0] instanceof String) {
          zeroElement = (String) opArgs[0];
          assert opArgs[1] instanceof TwoStringsToString;
          lambda = (TwoStringsToString) opArgs[1];
        } else {
          zeroElement = req.queryParams("zeroElement");
          assert opArgs[0] instanceof TwoStringsToString;
          lambda = (TwoStringsToString) opArgs[0];
        }
      } else {
        zeroElement = req.queryParams("zeroElement");
        assert parsedArgs instanceof TwoStringsToString;
        lambda = (TwoStringsToString) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          try {
            throw new OperationNotSupportedException();
          } catch (OperationNotSupportedException e) {
            throw new RuntimeException(e);
          }
        }
      };
      FlameRDDImpl flamePairRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
      return flamePairRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
          .reduce(zeroElement, lambda::op);
    });

    post(Operation.FOLD_BY_KEY.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      String zeroElement;
      TwoStringsToString lambda;
      if (parsedArgs instanceof Object[] opArgs) {
        if (opArgs.length >= 2 && opArgs[0] instanceof String) {
          zeroElement = (String) opArgs[0];
          assert opArgs[1] instanceof TwoStringsToString;
          lambda = (TwoStringsToString) opArgs[1];
        } else {
          zeroElement = req.queryParams("zeroElement");
          assert opArgs[0] instanceof TwoStringsToString;
          lambda = (TwoStringsToString) opArgs[0];
        }
      } else {
        zeroElement = req.queryParams("zeroElement");
        assert parsedArgs instanceof TwoStringsToString;
        lambda = (TwoStringsToString) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlamePairRDDImpl flamePairRDD = new FlamePairRDDImpl(context, req.queryParams("inputTable"));
      context.parallelizePairs(req.queryParams("outputTable"),
          flamePairRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
              .collect(Collectors.groupingByConcurrent(FlamePair::_1)).entrySet().stream().map(
                  kv -> new FlamePair(kv.getKey(),
                      kv.getValue().stream().map(FlamePair::_2).reduce(zeroElement, lambda::op))));
      return output.toString();
    });

    post(Operation.INTERSECTION.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      String otherTable;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof String;
        otherTable = (String) opArgs[0];
      } else {
        assert parsedArgs instanceof String;
        otherTable = (String) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
      FlameRDDImpl other = new FlameRDDImpl(context, otherTable);
      Set<String> otherSet = new LinkedHashSet<>(other.collect());
      context.parallelize(req.queryParams("outputTable"),
          flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
              .filter(otherSet::contains));
      return output.toString();
    });

    final Random random = new Random();

    post(Operation.SAMPLE.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      float p;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof Float;
        p = (Float) opArgs[0];
      } else {
        assert parsedArgs instanceof Float;
        p = (Float) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
      context.parallelize(req.queryParams("outputTable"),
          flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
              .filter(s -> random.nextDouble(0, 1) <= p));
      return output.toString();
    });

    post(Operation.DISTINCT.route, (req, res) -> {
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
      flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive")).forEach(v -> {
        try {
          context.getKVS().put(req.queryParams("outputTable"), v, "value", v);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      return output.toString();
    });

    post(Operation.FILTER.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      StringToBoolean predicate;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof StringToBoolean;
        predicate = (StringToBoolean) opArgs[0];
      } else {
        assert parsedArgs instanceof StringToBoolean;
        predicate = (StringToBoolean) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
      context.parallelize(req.queryParams("outputTable"),
          flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
              .filter(a -> {
                try {
                  return predicate.op(a);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }));
      return output.toString();
    });

    post(Operation.GROUP_BY.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      StringToString lambda;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof StringToString;
        lambda = (StringToString) opArgs[0];
      } else {
        assert parsedArgs instanceof StringToString;
        lambda = (StringToString) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
      context.parallelizePairs(req.queryParams("outputTable"),
          flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
              .collect(Collectors.groupingByConcurrent(s -> {
                try {
                  return lambda.op(s);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              })).entrySet().stream()
              .map(kv -> new FlamePair(kv.getKey(), String.join(",", kv.getValue()))));
      return output.toString();
    });

    post(Operation.FROM_TABLE.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      RowToString lambda;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof RowToString;
        lambda = (RowToString) opArgs[0];
      } else {
        assert parsedArgs instanceof RowToString;
        lambda = (RowToString) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };

      Iterator<Row> rows = context.getKVS()
          .scan(req.queryParams("inputTable"), req.queryParams("fromKey"),
              req.queryParams("toKeyExclusive"));
      context.parallelize(req.queryParams("outputTable"),
          StreamSupport.stream(((Iterable<Row>) () -> rows).spliterator(), true).map(lambda::op));
      return output.toString();
    });

    post(Operation.MAP_PARTITIONS.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      IteratorToIterator lambda;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof IteratorToIterator;
        lambda = (IteratorToIterator) opArgs[0];
      } else {
        assert parsedArgs instanceof IteratorToIterator;
        lambda = (IteratorToIterator) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlameRDDImpl flameRDD = new FlameRDDImpl(context, req.queryParams("inputTable"));
      context.parallelize(req.queryParams("outputTable"),
          StreamSupport.stream(((Iterable<String>) () -> {
            try {
              return lambda.op(
                  flameRDD.stream(req.queryParams("fromKey"), req.queryParams("toKeyExclusive"))
                      .iterator());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }).spliterator(), true));
      return output.toString();
    });

    post(Operation.JOIN.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      String otherTable;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof String;
        otherTable = (String) opArgs[0];
      } else {
        assert parsedArgs instanceof String;
        otherTable = (String) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };
      FlamePairRDDImpl flameRDD = new FlamePairRDDImpl(context, req.queryParams("inputTable"));
      FlamePairRDDImpl other = new FlamePairRDDImpl(context, otherTable);
      String fromKey = req.queryParams("fromKey");
      String toKeyExclusive = req.queryParams("toKeyExclusive");
      Map<String, List<FlamePair>> otherPairs = other.stream(fromKey, toKeyExclusive)
          .collect(Collectors.groupingByConcurrent(FlamePair::_1));
      context.parallelizePairs(req.queryParams("outputTable"),
          flameRDD.stream(fromKey, toKeyExclusive).flatMap(p1 -> otherPairs.get(p1._1()).stream()
              .map(p2 -> new FlamePair(p1._1(), p1._2() + "," + p2._2()))));
      return output.toString();
    });

    post(Operation.COGROUP.route, (req, res) -> {
      Object parsedArgs = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
      String otherTable;
      if (parsedArgs instanceof Object[] opArgs) {
        assert opArgs[0] instanceof String;
        otherTable = (String) opArgs[0];
      } else {
        assert parsedArgs instanceof String;
        otherTable = (String) parsedArgs;
      }
      KVSClient kvsClient = new KVSClient(req.queryParams("kvsMaster"));
      StringBuilder output = new StringBuilder();
      FlameContextImpl context = new FlameContextImpl(null, myJAR.getName()) {
        @Override
        public KVSClient getKVS() {
          return kvsClient;
        }

        @Override
        public void output(String s) {
          output.append(s);
        }
      };

      String fromKey = req.queryParams("fromKey");
      String toKeyExclusive = req.queryParams("toKeyExclusive");
      Map<String, List<FlamePair>> pairs = new FlamePairRDDImpl(context,
          req.queryParams("inputTable")).stream(fromKey, toKeyExclusive)
          .collect(Collectors.groupingByConcurrent(FlamePair::_1));
      Map<String, List<FlamePair>> otherPairs = new FlamePairRDDImpl(context, otherTable).stream(
          fromKey, toKeyExclusive).collect(Collectors.groupingByConcurrent(FlamePair::_1));
      Set<String> keys = new LinkedHashSet<>();
      keys.addAll(pairs.keySet());
      keys.addAll(otherPairs.keySet());
      context.parallelizePairs(keys.stream().map(k -> new FlamePair(k, "\"[%s],[%s]\"".formatted(
          String.join(",",
              () -> pairs.get(k).stream().map(FlamePair::_2).map(CharSequence.class::cast)
                  .iterator()), String.join(",",
              () -> otherPairs.get(k).stream().map(FlamePair::_2).map(CharSequence.class::cast)
                  .iterator())))));
      return output.toString();
    });
  }
}
