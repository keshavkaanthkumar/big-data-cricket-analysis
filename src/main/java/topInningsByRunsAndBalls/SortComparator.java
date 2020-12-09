package topInningsByRunsAndBalls;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {
    public SortComparator() {
        super(CompositeKey.class, true);
    }
    public int compare(WritableComparable a, WritableComparable b)
    {
        CompositeKey ck1 = (CompositeKey)a;
        CompositeKey ck2 = (CompositeKey)b;
        
   	   int result = (Integer.parseInt(ck1.getScore()) < Integer.parseInt(ck2.getScore())  ? 1 : Integer.parseInt(ck1.getScore())  == Integer.parseInt(ck2.getScore())  ? 0 : -1);
       // int result = ck1.getScore().compareTo(ck2.g); 
        if(result==0)
        {
        	 result = -1*Integer.parseInt(ck1.getBallsFaced()) < Integer.parseInt(ck2.getBallsFaced())  ? 1 : Integer.parseInt(ck1.getBallsFaced())  == Integer.parseInt(ck2.getBallsFaced())  ? 0 : -1;
             return result;
        }
        return result;
    }
}
