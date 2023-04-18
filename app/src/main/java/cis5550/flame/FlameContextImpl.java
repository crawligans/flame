package cis5550.flame;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.HTTP.Response;
import cis5550.tools.Logger;
import cis5550.tools.Partitioner;
import cis5550.tools.Partitioner.Partition;
import cis5550.tools.Serializer;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.naming.OperationNotSupportedException;

public abstract class FlameContextImpl implements FlameContext, Serializable {

  private static final Logger logger = Logger.getLogger(FlameContextImpl.class);
  transient private final Partitioner partitioner;
  private List<Partition> partitions;

  public FlameContextImpl(Partitioner partitioner, String jarName) {
    this.partitioner = partitioner;
  }

  public static void parallelize(KVSClient client, String table, Stream<String> list) {
    list.forEach(v -> {
      try {
        client.put(table, UUID.randomUUID().toString(), "value", v);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static void parallelizePairs(KVSClient client, String table, Stream<FlamePair> pairs) {
    pairs.forEach(pair -> {
      try {
        client.put(table, pair._1(), UUID.randomUUID().toString(), pair._2());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static void parallelize(KVSClient client, String table, List<String> list)
      throws Exception {
    for (String v : list) {
      client.put(table, UUID.randomUUID().toString(), "value", v);
    }
  }

  public static void parallelizePairs(KVSClient client, String table, Collection<FlamePair> pairs)
      throws Exception {
    for (FlamePair pair : pairs) {
      client.put(table, pair._1(), UUID.randomUUID().toString(), pair._2());
    }
  }

  @Override
  public abstract KVSClient getKVS();

  @Override
  public abstract void output(String s);


  public FlameRDD parallelize(String table, Stream<String> list) throws Exception {
    KVSClient client = getKVS();
    parallelize(client, table, list);
    return new FlameRDDImpl(this, table);
  }

  public FlamePairRDD parallelizePairs(String table, Stream<FlamePair> pairs) throws Exception {
    KVSClient client = getKVS();
    parallelizePairs(client, table, pairs);
    return new FlamePairRDDImpl(this, table);
  }

  public FlameRDD parallelize(String table, List<String> list) throws Exception {
    KVSClient client = getKVS();
    parallelize(client, table, list);
    return new FlameRDDImpl(this, table);
  }

  public FlamePairRDD parallelizePairs(String table, Collection<FlamePair> pairs) throws Exception {
    KVSClient client = getKVS();
    parallelizePairs(client, table, pairs);
    return new FlamePairRDDImpl(this, table);
  }

  @Override
  public FlameRDD parallelize(List<String> list) throws Exception {
    return parallelize(UUID.randomUUID().toString(), list);
  }

  public FlamePairRDD parallelizePairs(Collection<FlamePair> pairs) throws Exception {
    return parallelizePairs(UUID.randomUUID().toString(), pairs);
  }

  public FlameRDD parallelize(Stream<String> list) throws Exception {
    return parallelize(UUID.randomUUID().toString(), list);
  }

  public FlamePairRDD parallelizePairs(Stream<FlamePair> pairs) throws Exception {
    return parallelizePairs(UUID.randomUUID().toString(), pairs);
  }

  public String invokeOperation(Operation operation, String inputTable, Object... args)
      throws IOException {
    String outputTable = UUID.randomUUID().toString();
    if (partitions == null) {
      partitions = partitioner.assignPartitions();
    }
    List<Response> responses = partitions.parallelStream().map(part -> {
      try {
        return HTTP.doRequest("POST", new URL(
            "http://" + part.assignedFlameWorker + operation.route + "?" + String.join("&",
                (part.fromKey != null ? "fromKey=" + URLEncoder.encode(part.fromKey,
                    StandardCharsets.UTF_8) : ""),
                (part.toKeyExclusive != null ? "toKeyExclusive=" + URLEncoder.encode(
                    part.toKeyExclusive, StandardCharsets.UTF_8) : ""),
                "inputTable=" + URLEncoder.encode(inputTable, StandardCharsets.UTF_8),
                "outputTable=" + URLEncoder.encode(outputTable, StandardCharsets.UTF_8),
                "kvsMaster=" + URLEncoder.encode(getKVS().getMaster(),
                    StandardCharsets.UTF_8))).toString(), Serializer.objectToByteArray(args));
      } catch (IOException e) {
        return new Response(e.toString().getBytes(StandardCharsets.UTF_8), new HashMap<>(), 500);
      }
    }).peek(res -> {
      if (res.statusCode() < 400) {
        logger.info(new String(res.body()));
      } else {
        logger.error(new String(res.body()));
      }
    }).toList();
    return switch (operation) {
      case FOLD -> responses.stream().map(res -> new String(res.body(), StandardCharsets.UTF_8))
          .reduce((String) args[0], ((TwoStringsToString) args[1])::op);
      default -> outputTable;
    };
  }

  @Override
  public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
    return new FlameRDDImpl(this, this.invokeOperation(Operation.FROM_TABLE, tableName, lambda));
  }

  @Override
  public void setConcurrencyLevel(int keyRangesPerWorker) {
    try {
      throw new OperationNotSupportedException();
    } catch (OperationNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  enum FlameRDDType {
    STRING(FlameRDD.class), PAIR(FlamePairRDD.class);
    final Class<?> clazz;

    FlameRDDType(Class<?> clazz) {
      this.clazz = clazz;
    }

    public Class<?> getRDDClass() {
      return clazz;
    }
  }

  enum Operation {
    FLAT_MAP("/rdd/flatMap"), FLAT_MAP_TO_PAIR("/rdd/flatMapToPair"), MAP_TO_PAIR(
        "/rdd/mapToPair"), FOLD_BY_KEY("/rdd/foldByKey"), INTERSECTION("/rdd/intersection"), SAMPLE(
        "/rdd/sample"), GROUP_BY("/rdd/groupBy"), FROM_TABLE("/rdd/fromTable"), DISTINCT(
        "/rdd/distinct"), JOIN("/rdd/join"), FOLD("/rdd/fold"), FILTER(
        "/rdd/filter"), MAP_PARTITIONS("/rdd/mapPartitions"), COGROUP("/rdd/cogroup");
    public final String route;

    Operation(String route) {
      this.route = route;
    }
  }
}
