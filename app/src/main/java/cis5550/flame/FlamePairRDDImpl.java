package cis5550.flame;

import cis5550.flame.FlameContextImpl.Operation;
import cis5550.kvs.Row;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.naming.OperationNotSupportedException;

public class FlamePairRDDImpl implements FlamePairRDD {

  private final FlameContextImpl context;
  private String table;
  private boolean saved = false;

  public FlamePairRDDImpl(FlameContextImpl context, String table) {
    this.context = context;
    this.table = table;
  }

  public Stream<FlamePair> stream() throws Exception {
    Iterator<Row> rows = context.getKVS().scan(table);
    return StreamSupport.stream(((Iterable<Row>) () -> rows).spliterator(), true)
        .flatMap(r -> r.columns().stream().map(c -> new FlamePair(r.key(), r.get(c))));
  }

  public Stream<FlamePair> stream(String fromKey, String toKeyExclusive) throws Exception {
    Iterator<Row> rows = context.getKVS().scan(table, fromKey, toKeyExclusive);
    return StreamSupport.stream(((Iterable<Row>) () -> rows).spliterator(), true)
        .flatMap(r -> r.columns().stream().map(c -> new FlamePair(r.key(), r.get(c))));
  }

  @Override
  public List<FlamePair> collect() throws Exception {
    return stream().collect(Collectors.toCollection(ArrayList::new));
  }

  @Override
  public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
    return new FlamePairRDDImpl(context,
        context.invokeOperation(Operation.FOLD_BY_KEY, table, zeroElement, lambda));
  }

  @Override
  public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
    return new FlameRDDImpl(context, context.invokeOperation(Operation.FLAT_MAP, table, lambda));
  }

  @Override
  public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
    return new FlamePairRDDImpl(context,
        context.invokeOperation(Operation.FLAT_MAP_TO_PAIR, table, lambda));
  }

  @Override
  public FlamePairRDD join(FlamePairRDD other) throws Exception {
    String otherTable = UUID.randomUUID().toString();
    other.saveAsTable(otherTable);
    return new FlamePairRDDImpl(context,
        context.invokeOperation(Operation.JOIN, table, otherTable));
  }

  @Override
  public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
    throw new OperationNotSupportedException();
  }

  @Override
  public void saveAsTable(String tableNameArg) throws Exception {
    context.getKVS().rename(table, tableNameArg);
    this.table = tableNameArg;
    this.saved = true;
  }

  @Override
  public String drop() throws Exception {
    return drop(false);
  }

  @Override
  public String drop(boolean saved) throws Exception {
    if (this.saved && !saved) {
      throw new IllegalStateException("Use the 'saved' argument to confirm delete");
    }
    context.getKVS().delete(table);
    return null;
  }
}
