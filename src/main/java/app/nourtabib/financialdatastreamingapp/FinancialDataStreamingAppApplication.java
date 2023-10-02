package app.nourtabib.financialdatastreamingapp;

import app.nourtabib.financialdatastreamingapp.avros.AccountActivityAggregate;
import app.nourtabib.financialdatastreamingapp.streams.utils.Transformers;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

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
			Transformers transformers = new Transformers();
			Instant now = Instant.ofEpochSecond(0);
//			System.out.println(now.atZone(ZoneId.of("UTC")).getDayOfMonth());
			List<AccountActivityAggregate> initialList = new ArrayList<>(5);
			List<Integer> sortedList = new ArrayList<>(24);
			for (int i = 0; i < 30; i++) {
				sortedList.add(0);
			}
			initialList.add(AccountActivityAggregate.newBuilder().setTimestamp(now).setCount(1).setTotal(10).build());
			initialList.add(AccountActivityAggregate.newBuilder().setTimestamp(now.plusSeconds(24L*60L*60L)).setCount(2).setTotal(20).build());
			initialList.add(AccountActivityAggregate.newBuilder().setTimestamp(now.plusSeconds(2L*24L*60L*60L)).setCount(3).setTotal(30).build());
			initialList.add(AccountActivityAggregate.newBuilder().setTimestamp(now.plusSeconds(3L*24L*60L*60L)).setCount(4).setTotal(40).build());
			initialList.add(AccountActivityAggregate.newBuilder().setTimestamp(now.plusSeconds(5L*24L*60L*60L)).setCount(5).setTotal(50).build());


			initialList=initialList.stream().sorted((x,y)-> Integer.compare(x.getTimestamp().atZone(ZoneId.of("UTC")).getDayOfMonth(),y.getTimestamp().atZone(ZoneId.of("UTC")).getDayOfMonth())).toList();
			for(AccountActivityAggregate c : initialList){
				int day = c.getTimestamp().atZone(ZoneId.of("UTC")).getDayOfMonth();
				if (day > 0 && day < 30) {
					sortedList.set(day-1, c.getCount());
				}
			}
//			for(int c : sortedList){
//				System.out.println(c);
//			}
			double[] a = transformers.extractTotalsFromWindowActivity.apply(initialList,181,"daily");
//			System.out.println(Arrays.toString(a));
			System.out.println(a.length);

//			double [] res =  Transformers.stfTransform.apply(a,30,29);
			double [] res1 =  Transformers.stfTransform.apply(a,60,59);
//			System.out.println(Arrays.toString(res1));
//			System.out.println(res1.length);
//			SpringApplication.run(FinancialDataStreamingAppApplication.class, args);

		}

}
