package hadoop.workcount.comparable;

import lombok.Getter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Auther Chris Lee
 * @Date 3/28/2019 16:26
 * @Description
 */
@Getter
public class KeyComparator implements WritableComparable<KeyComparator> {
	
	private String key;
	
	KeyComparator(String key)
	{
		this.key = key;
	}
	
	@Override
	public int compareTo(KeyComparator o) {
		return key.compareTo(o.key);
	}
	
	@Override
	public void write(DataOutput out)
		throws IOException {
		out.writeBytes(key);
	}
	
	@Override
	public void readFields(DataInput in)
		throws IOException {
		key = in.readLine();
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		KeyComparator that = (KeyComparator)o;
		return Objects.equals(key, that.key);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(key);
	}
	
}
