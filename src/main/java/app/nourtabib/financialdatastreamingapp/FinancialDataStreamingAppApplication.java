package app.nourtabib.financialdatastreamingapp;

import app.nourtabib.financialdatastreamingapp.avros.RawTransactionAvro;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class FinancialDataStreamingAppApplication {

//	@Component
//	public class Container {
//		@Autowired
//		public Container(KafkaTemplate<String, RawTransactionAvro> kafkaTemplate) {
//
//			double senderBalance = 18000;
//			double receiverBalance = 0;
//			Instant start = Instant.parse("1970-01-02T02:00:00Z");
//			Instant current = start;
//			Instant end = Instant.parse("1970-01-03T04:00:00Z");
//			int i = 1;
//			while (current.isBefore(end)) {
//				senderBalance = senderBalance - 1;
//				RawTransactionAvro record = RawTransactionAvro.newBuilder()
//						.setTransactionId(UUID.randomUUID().toString())
//						.setAmount(1)
//						.setTimestamp(current)
//						.setNameDest("Nour")
//						.setNameOrig("Ahmed")
//						.setNewBalanceOrig(1)
//						.setOldBalanceOrg(1)
//						.setOldBalanceDest(1)
//						.setNewBalanceDest(1)
//						.setType("Transfert")
//						.build();
//				CompletableFuture<SendResult<String, RawTransactionAvro>> future = kafkaTemplate.send(
//						"raw-transaction-events",
//						record.getTransactionId().toString(),
//						record
//				);
//				System.out.println("SENDING MESSAGE : "+i);
//				RawTransactionAvro result = future.join().getProducerRecord().value();
//				current = current.plusSeconds(5).plusNanos(30);
//				i = i+1;
//			}
//		}
//	}
		public static void main(String[] args) {
			SpringApplication.run(FinancialDataStreamingAppApplication.class, args);

		}

}
