package com.demo.cloud.springcloudkafkaproducer;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
public class KafkaProducerApplication {

	@Slf4j
	@Component
	public static class PageViewEventSource implements ApplicationRunner {

		private final MessageChannel pageViewsOut;

		public PageViewEventSource(AnalyticsBinding binding) {
			this.pageViewsOut = binding.pageViewsOut();
		}
	
		@Override
		public void run(ApplicationArguments args) throws Exception {
			List<String> names = Arrays.asList("tom", "mouse", "jerry", "cat", "dog");
			List<String> pages = Arrays.asList("home", "sitemap", "about", "blog", "news");

			Runnable runnable = () -> {
				String rPage = pages.get(new Random().nextInt(pages.size()));
				String rName = pages.get(new Random().nextInt(names.size()));

				PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);

				Message<PageViewEvent> message = MessageBuilder
					.withPayload(pageViewEvent)
					.setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
					.build();

				try {
					this.pageViewsOut.send(message);
					log.info("send message: " + message);
				} catch (Exception ex) {
					log.error(ex.getMessage());
				}
			};

			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 5, 5, TimeUnit.SECONDS);
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerApplication.class, args);
	}

}

/**
 * AnalyticsBinding
 */
interface AnalyticsBinding {

	String PAGE_VIEW_OUT = "pvout";

	@Output(PAGE_VIEW_OUT)
	MessageChannel pageViewsOut();	
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
	private long duration;	
}
