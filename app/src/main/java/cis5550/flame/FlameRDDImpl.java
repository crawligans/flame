package cis5550.flame;

import cis5550.flame.FlameContextImpl.Operation;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.Row;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FlameRDDImpl implements FlameRDD {


  private final FlameContextImpl context;
  private String table;

  public FlameRDDImpl(FlameContextImpl context, String table) {
    this.context = context;
    this.table = table;
  }

  @Override
  public int count() throws Exception {
    return context.getKVS().count(table);
  }

  @Override
  public void saveAsTable(String tableNameArg) throws Exception {
    context.getKVS().rename(table, tableNameArg);
    this.table = tableNameArg;
  }

  @Override
  public FlameRDD distinct() throws Exception {
    return new FlameRDDImpl(context, context.invokeOperation(Operation.DISTINCT, table));
  }

  @Override
  public Vector<String> take(int num) throws Exception {
    return stream().limit(num).collect(Collectors.toCollection(Vector::new));
  }

  @Override
  public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
    return context.invokeOperation(Operation.FOLD, table, zeroElement, lambda);
  }

  @Override
  public List<String> collect() throws Exception {
    return stream().collect(Collectors.toCollection(ArrayList::new));
  }

  public Stream<String> stream(String fromKey, String toKeyExclusive) throws Exception {
    Iterator<Row> rows = context.getKVS().scan(table, fromKey, toKeyExclusive);
    return StreamSupport.stream(((Iterable<Row>) () -> rows).spliterator(), true)
        .map(r -> r.get("value"));
  }

  public Stream<String> stream() throws Exception {
    Iterator<Row> rows = context.getKVS().scan(table);
    return StreamSupport.stream(((Iterable<Row>) () -> rows).spliterator(), true)
        .map(r -> r.get("value"));
  }

  @Override
  public FlameRDD flatMap(StringToIterable lambda) throws Exception {
    return new FlameRDDImpl(context, context.invokeOperation(Operation.FLAT_MAP, table, lambda));
  }

  @Override
  public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
    return new FlamePairRDDImpl(context,
        context.invokeOperation(Operation.FLAT_MAP_TO_PAIR, table, lambda));
  }

  @Override
  public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
    return new FlamePairRDDImpl(context,
        context.invokeOperation(Operation.MAP_TO_PAIR, table, lambda));
  }

  @Override
  public FlameRDD intersection(FlameRDD r) throws Exception {
    String otherTable = UUID.randomUUID().toString();
    r.saveAsTable(otherTable);
    return new FlameRDDImpl(context,
        context.invokeOperation(Operation.INTERSECTION, table, otherTable));
  }

  @Override
  public FlameRDD sample(double f) throws Exception {
    return new FlameRDDImpl(context, context.invokeOperation(Operation.SAMPLE, table, f));
  }

  @Override
  public FlamePairRDD groupBy(StringToString lambda) throws Exception {
    return new FlamePairRDDImpl(context,
        context.invokeOperation(Operation.GROUP_BY, table, lambda));
  }

  @Override
  public FlameRDD filter(StringToBoolean lambda) throws Exception {
    return new FlameRDDImpl(context, context.invokeOperation(Operation.FILTER, table, lambda));
  }

  @Override
  public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
    return new FlameRDDImpl(context,
        context.invokeOperation(Operation.MAP_PARTITIONS, table, lambda));
  }
}
