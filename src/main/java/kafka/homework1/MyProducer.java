package kafka.homework1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * @Auther Chris Lee
 * @Date 3/20/2019 17:45
 * @Description
 */
public class MyProducer {
	private static List<String> contents;
	
	static {
		List<String> temp = new ArrayList();
		temp.add("Hello world");
		temp.add("Hello java");
		temp.add("Hello food");
		temp.add("Hello tomorrow");
		contents = Collections.unmodifiableList(temp);
	}
	
	public void produceMessage() {
		Properties props = getConfig();
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		for (int i = 0; i < contents.size(); i++) {
			System.out.println("i:" + contents.get(i));
			producer.send(new ProducerRecord<>("test", Integer.toString(i), contents.get(i)));
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		producer.close();
	}
	
	// config
	public Properties getConfig() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}
	
	public static void main(String[] args) {
		MyProducer myProducer = new MyProducer();
		myProducer.produceMessage();
	}
}
