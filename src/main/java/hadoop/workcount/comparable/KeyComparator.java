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
	
	private int value;
	
	KeyComparator(String key, int value)
	{
		this.key = key;
		this.value = value;
	}
	
	@Override
	public int compareTo(KeyComparator o) {
		return key.compareTo(o.key);
	}
	
	@Override
	public void write(DataOutput out)
		throws IOException {
		out.writeBytes(key);
		out.writeInt(value);
	}
	
	@Override
	public void readFields(DataInput in)
		throws IOException {
		key = in.readLine();
		value = in.readInt();
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		KeyComparator that = (KeyComparator)o;
		return Objects.equals(key, that.key) && Objects.equals(value, that.value);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(key, value);
	}
}
