package kafka.homework1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Auther Chris Lee
 * @Date 3/20/2019 17:44
 * @Description
 */
public class MyConsumer {
	
	// config
	public static Properties getConfig() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "testGroup");
		props.put("enable.auto.commit", "true");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		return props;
	}
	
	public void consumeMessage() {
		
		final String topic = "test";
		
		// launch 1 thread to consume
		int threads_amount = 1;
		
		final ExecutorService executor = Executors.newFixedThreadPool(threads_amount);
		final List<KafkaConsumerRunner> consumers = new ArrayList<>();
		
		for (int i = 0; i < threads_amount; i++) {
			KafkaConsumerRunner consumer = new KafkaConsumerRunner(topic);
			consumers.add(consumer);
			executor.submit(consumer);
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (KafkaConsumerRunner consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	// Thread to consume kafka data
	public static class KafkaConsumerRunner implements Runnable {
		private final AtomicBoolean closed = new AtomicBoolean(false);
		
		private final KafkaConsumer<String, String> consumer;
		
		private final String topic;
		
		public KafkaConsumerRunner(String topic)
		{
			Properties props = getConfig();
			consumer = new KafkaConsumer<>(props);
			this.topic = topic;
		}
		
		public void handleRecord(ConsumerRecord record) {
			System.out.println("name: " + Thread.currentThread().getName() + " ; topic: " + record.topic() + " ; offset" + record.offset()
				+ " ; key: " + record.key() + " ; value: " + record.value());
		}
		
		public void run() {
			try {
				// subscribe
				consumer.subscribe(Arrays.asList(topic));
				while (!closed.get()) {
					// read data
					ConsumerRecords<String, String> records = consumer.poll(10000);
					// Handle new records
					for (ConsumerRecord<String, String> record : records) {
						handleRecord(record);
					}
				}
			}
			catch (WakeupException e) {
				// Ignore exception if closing
				if (!closed.get()) {
					throw e;
				}
			}
			finally {
				consumer.close();
			}
		}
		
		// Shutdown hook which can be called from a separate thread
		public void shutdown() {
			closed.set(true);
			consumer.wakeup();
		}
	}
	
	public static void main(String[] args) {
		MyConsumer myConsumer = new MyConsumer();
		myConsumer.consumeMessage();
	}
	
}
