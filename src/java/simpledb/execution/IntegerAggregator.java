package simpledb.execution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private GBHandler gbHandler;


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        initHandler();
    }

    private void initHandler() {
        switch (this.what) {
            case AVG:
                gbHandler = new AvgHandler();
                break;
            case COUNT:
                gbHandler = new CountHandler();
                break;
            case MAX:
                gbHandler = new MaxHandler();
                break;
            case MIN:
                gbHandler = new MinHandler();
                break;
            case SUM:
                gbHandler = new SumHandler();
                break;
            default:
                throw new IllegalArgumentException("wrong op");
        }
    }

    private final static String NO_GROUPING_KEY = "NO_GROUPING_KEY";
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfieldtype != null && !tup.getField(gbfield).getType().equals(gbfieldtype)) {
            throw new IllegalArgumentException("gbfieldtype has wrong type");
        }
        String key = null;
        if(gbfield == NO_GROUPING) {
           key = NO_GROUPING_KEY;
        } else {
            key = tup.getField(gbfield).toString();
        }
        gbHandler.handle(key, tup.getField(afield));
    }

    private abstract class GBHandler {
        ConcurrentHashMap<String, Integer> gbResult;
        abstract void handle(String key, Field field);
        private GBHandler() {
            gbResult = new ConcurrentHashMap<>();
        }

        public Map<String, Integer> getGbResult() {
            return gbResult;
        }
    }

    private class CountHandler extends GBHandler {
        @Override
        void handle(String key, Field field) {
            if(gbResult.containsKey(key)) {
                gbResult.put(key, gbResult.get(key) + 1);
            } else {
                gbResult.put(key, 1);
            }
        }
    }

    private class SumHandler extends GBHandler {
        @Override
        void handle(String key, Field field) {
            if(gbResult.containsKey(key)) {
                gbResult.put(key, gbResult.get(key) + Integer.parseInt(field.toString()));
            } else {
                gbResult.put(key, Integer.parseInt(field.toString()));
            }
        }
    }

    private class MinHandler extends GBHandler {
        @Override
        void handle(String key, Field field) {
            if(gbResult.containsKey(key)) {
                gbResult.put(key, Math.min(gbResult.get(key), Integer.parseInt(field.toString())));
            } else {
                gbResult.put(key, Integer.parseInt(field.toString()));
            }
        }
    }

    private class MaxHandler extends GBHandler {
        @Override
        void handle(String key, Field field) {
            if(gbResult.containsKey(key)) {
                gbResult.put(key, Math.max(gbResult.get(key), Integer.parseInt(field.toString())));
            } else {
                gbResult.put(key, Integer.parseInt(field.toString()));
            }
        }
    }

    private class AvgHandler extends GBHandler {
        ConcurrentHashMap<String, Integer> count;
        ConcurrentHashMap<String, Integer> sum;
        @Override
        void handle(String key, Field field) {
            int val = Integer.parseInt(field.toString());
            if(gbResult.containsKey(key)) {
                count.put(key, count.get(key) + 1);
                sum.put(key, sum.get(key) + val);
            } else {
                count.put(key, 1);
                sum.put(key, val);
            }
            gbResult.put(key, sum.get(key) / count.get(key)); //TODO 当前只精确到整数
        }
        private AvgHandler() {
            count = new ConcurrentHashMap<>();
            sum = new ConcurrentHashMap<>();
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Map<String, Integer> gbResult = gbHandler.getGbResult();
        TupleDesc td = null;
        List<Tuple> tuples = new ArrayList<>();
        if(gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateVal"});
            for(Integer val : gbResult.values()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(val));
                tuples.add(tuple);
            }
        } else {
            td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, new String[] {"groupVal", "aggregateVal"});
            for(Map.Entry<String, Integer> entry : gbResult.entrySet()) {
                Tuple tuple = new Tuple(td);
                if(gbfieldtype.equals(Type.INT_TYPE)) {
                    tuple.setField(0, new IntField(Integer.parseInt(entry.getKey())));
                } else {
                    tuple.setField(0, new StringField(entry.getKey(), entry.getKey().length()));
                }
                tuple.setField(1, new IntField(entry.getValue()));
                tuples.add(tuple);
            }

        }
        return new TupleIterator(td, tuples);
    }

}
