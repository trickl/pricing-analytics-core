package com.trickl.flux.transformers.pricing;

import com.trickl.model.pricing.primitives.Candle;
import com.trickl.model.pricing.primitives.Candle.CandleBuilder;
import com.trickl.model.pricing.primitives.OrderBook;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class OrderBookToCandleTransformer
    implements BiFunction<Publisher<Instant>, Publisher<OrderBook>, Flux<Candle>> {

  private final Duration candleWidth;
  private final Function<OrderBook, Optional<BigDecimal>> priceFunction;

  @Override
  public Flux<Candle> apply(Publisher<Instant> time, Publisher<OrderBook> orderBook) {

    Candle.CandleBuilder candleBuilder = Candle.builder();    
    
    return Flux.<Instant, OrderBook, Publisher<Candle>>combineLatest(
            time, orderBook, (t, q) -> buildCandles(t, q, candleBuilder))
        .flatMap(Flux::from);
  }

  private Publisher<Candle> buildCandles(
      Instant time,
      OrderBook orderBook,
      CandleBuilder candleBuilder) {
    
    Instant latestTime = time.isAfter(orderBook.getTime()) ? time : orderBook.getTime();
    Optional<BigDecimal> price = priceFunction.apply(orderBook);
    
    Publisher<Candle> candles = updateTime(latestTime, candleBuilder);

    if (price.isPresent()) {      
      Candle lastCandle = candleBuilder.build();
      if (lastCandle.getTime() == null) {
        IntervalEnd intervalEnd = new IntervalEnd(candleWidth);        
        Instant candleEnd = intervalEnd.apply(time);
        buildCandle(candleBuilder, price.get(), candleEnd); 
      }

      if (lastCandle.getHigh() == null || price.get().compareTo(lastCandle.getHigh()) > 0) {
        candleBuilder.high(price.get());
      }

      if (lastCandle.getLow() == null || price.get().compareTo(lastCandle.getLow()) < 0) {
        candleBuilder.low(price.get());
      }

      candleBuilder.close(price.get());

      Candle incompleteCandle = candleBuilder.complete(false).build();
      candles = Flux.merge(candles, Mono.just(incompleteCandle));
    }    

    return candles;
  }

  private Publisher<Candle> updateTime(
      Instant time,
      CandleBuilder candleBuilder) {
    IntervalEnd intervalEnd = new IntervalEnd(candleWidth);
    Candle lastCandle = candleBuilder.build();
    Publisher<Candle> completedCandles = Flux.empty();

    Instant newCandleEnd = intervalEnd.apply(time);
    Instant lastCandleTime = lastCandle.getTime();

    if (lastCandleTime != null) {
      while (lastCandleTime.isBefore(newCandleEnd)) {
        lastCandle = candleBuilder.complete(true).build();

        if (lastCandle.getClose() != null) {
          completedCandles = Flux.merge(completedCandles, Mono.just(lastCandle));
        }

        // Begin a new candle
        BigDecimal closePrice = lastCandle.getClose();
        lastCandleTime = intervalEnd.apply(lastCandle.getTime());
        buildCandle(candleBuilder, closePrice, lastCandleTime);
      }
    }

    return completedCandles;
  }

  private void buildCandle(CandleBuilder candleBuilder, BigDecimal price, Instant time) {
    candleBuilder
      .open(price)
      .high(price)
      .low(price)
      .close(price)
      .time(time);
  }
}
