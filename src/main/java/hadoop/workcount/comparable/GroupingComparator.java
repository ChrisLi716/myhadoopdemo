package hadoop.workcount.comparable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Auther Chris Lee
 * @Date 3/28/2019 17:08
 * @Description
 */
public class GroupingComparator extends WritableComparator {
	
	protected GroupingComparator()
	{
		super(KeyComparator.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		KeyComparator ip1 = (KeyComparator)w1;
		KeyComparator ip2 = (KeyComparator)w2;
		int l = ip1.getValue();
		int r = ip2.getValue();
		return Integer.compare(l, r);
	}
}
