package topInningsByRunsAndBalls;

import org.apache.hadoop.io.WritableComparator;


public class SortGroupComparator extends WritableComparator {
    public SortGroupComparator() {
        super(CompositeKey.class,true);
    }
    @Override
    public int compare(Object a, Object b)
    {
        CompositeKey ck1 = (CompositeKey)a;
        CompositeKey ck2 = (CompositeKey)b;
        return (Integer.parseInt(ck1.getScore()) < Integer.parseInt(ck2.getScore())  ? 1 : Integer.parseInt(ck1.getScore())  == Integer.parseInt(ck2.getScore())  ? 0 : -1);
    }
}
