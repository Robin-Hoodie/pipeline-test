package io.oreon.crypto;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CryptoPipeline {

    private static final Logger logger = LoggerFactory.getLogger(CryptoPipeline.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.setTempLocation("gs://dataflow-cpa-bucket/tmp/");
        Pipeline p = Pipeline.create();
        p.apply(
                "read_stamp",
                TextIO.read().from("gs://cpa-pubsub-dumps-bitstamp-btcusd/ingest_raw_bitstamp_btcusd_live_trades_trade/2019-07-18-20-08-28/00000.json")
        )
                .apply("split_tweets", ParDo.of(new SplitFileToPubsubMessagesDoFn()))
                .apply("AddTimeStamp", ParDo.of(new SetBitstampTimestampDoFn()))
                .apply("Window-60min-fixed", Window.into(FixedWindows.of(Duration.standardSeconds(60))))
                .apply("GetAmountOfTrade", ParDo.of(new DoFn<TradeData, Double>() {    // a DoFn as an anonymous inner class instance
                    @ProcessElement
                    public void processElement(@Element TradeData tradeData, OutputReceiver<Double> out) {
                        out.output(tradeData.amount);
                    }
                }))
                .apply("GroupAndSum", Combine.globally(Sum.ofDoubles()).withoutDefaults())
                .apply("Logging", ParDo.of(new DoFn<Double, String>() {
                    @ProcessElement
                    public void processElement(@Element Double sum, OutputReceiver<String> out) {
                        logger.info(sum.toString());
                    }
                }));
        p.run(options).waitUntilFinish();
    }



    private static class SplitFileToPubsubMessagesDoFn extends DoFn<String, TradeData> {
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<TradeData> out) {
            Gson gson = new Gson();
            List<Trade> trades = gson.fromJson(json, new TypeToken<List<Trade>>(){}.getType());
            trades.stream()
                    .map(trade -> trade.data)
                    .map(tradeDataAsString -> gson.fromJson(tradeDataAsString, TradeData.class))
                    .forEach(out::output);
        }
    }

    private static class SetBitstampTimestampDoFn extends DoFn<TradeData, TradeData> {

        @ProcessElement
        public void processElement(@Element TradeData tradeData, OutputReceiver<TradeData> out) {
            Instant timestamp = Instant.ofEpochMilli(Long.parseLong(tradeData.microtimestamp) / 1000);
            out.outputWithTimestamp(tradeData, timestamp);
        }
    }

    private static class Trade {
        public String publish_time;
        public String data;
        public String attributes;
        public String message_id;
    }

    @DefaultCoder(AvroCoder.class)
    private static class TradeData {
        public String microtimestamp;
        public double amount;
        public long buy_order_id;
        public long sell_order_id;
        public String amount_str;
        public String price_str;
        public String timestamp;
        public double price;
        public int type;
        public int id;

        @Override
        public String toString() {
            return "TradeData{" +
                    "microtimestamp='" + microtimestamp + '\'' +
                    ", amount=" + amount +
                    ", buy_order_id=" + buy_order_id +
                    ", sell_order_id=" + sell_order_id +
                    ", amount_str='" + amount_str + '\'' +
                    ", price_str='" + price_str + '\'' +
                    ", timestamp='" + timestamp + '\'' +
                    ", price=" + price +
                    ", type=" + type +
                    ", id=" + id +
                    '}';
        }
    }
}
