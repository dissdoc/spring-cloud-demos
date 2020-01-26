package com.demo.cloud.springcloudkafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableBinding(StreamReaderBinding.class)
public class KafkaConsumerApplication {

	@Component
	@Slf4j
	public static class PageViewEventSink {
		@StreamListener(StreamReaderBinding.PAGE_VIEWS_IN)
		public void handle(PageViewEvent event) {
			log.info(event.getPage());
		}
	// 	public void process(@Input(AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, PageViewEvent> events) {
	// 		String raw = events
	// 			.filter((key, value) -> value.getDiration() > 10)
	// 			.toString();

	// 		log.info(raw);
	// 	}
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}

}


/**
 * AnalyticsBinding
 */
interface StreamReaderBinding {

	String PAGE_VIEWS_IN = "pvin";
	
	@Input(PAGE_VIEWS_IN)
	SubscribableChannel pageViewsIn();
}

/**
 * PageViewEvent
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent {

	private String userId;
	private String page;
	private long diration;
}