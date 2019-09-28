package com.trickl.flux.transformers.pricing;

import com.trickl.model.pricing.primitives.Candle;
import com.trickl.model.pricing.primitives.OrderBook;
import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

public class OrderBookToCandleTransformerTest {

  private static final Instant START_TIME = Instant.parse("2019-01-01T12:00:00.00Z");

  private FluxSink<Instant> timeSink;
  private FluxSink<OrderBook> orderBookSink;
  private Flux<Candle> candleStream;

  /**
   * Setup the tests.
   */
  @Before
  public void setup() {
    OrderBookToCandleTransformer midTransformer =
        new OrderBookToCandleTransformer(Duration.ofMinutes(1), new OrderBookMid());
    EmitterProcessor<Instant> timeStream = EmitterProcessor.create();
    timeSink = timeStream.sink();
    EmitterProcessor<OrderBook> orderBookStream = EmitterProcessor.create();
    orderBookSink = orderBookStream.sink();
    candleStream = midTransformer.apply(timeStream, orderBookStream);
  }

  @Test
  public void testCreateNoCandleNoValue() {
    acceptHeartbeat(Duration.ofSeconds(0));

    StepVerifier.create(candleStream)
        .then(this::completeSources)
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void testCreatePartialCandleSingleValue() {

    StepVerifier.create(candleStream)
        .then(() -> acceptHeartbeat(Duration.ofSeconds(0)))
        .then(() -> acceptMidPrice(Duration.ofSeconds(1), 15))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, false))
        .then(this::completeSources)
        .expectComplete()
        .verify(Duration.ofSeconds(300));
  }

  @Test
  public void testCreatePartialCandleMultiValue() {
    StepVerifier.create(candleStream)
        .then(() -> acceptHeartbeat(Duration.ofSeconds(0)))
        .then(() -> acceptMidPrice(Duration.ofSeconds(1), 15))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, false))
        .then(() -> acceptMidPrice(Duration.ofSeconds(2), 16))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 16, 15, 16, false))
        .then(() -> acceptMidPrice(Duration.ofSeconds(3), 17))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 17, 15, 17, false))
        .then(() -> acceptMidPrice(Duration.ofSeconds(4), 15))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 17, 15, 15, false))
        .then(this::completeSources)
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void testCreateFullCandleMultiValue() {

    StepVerifier.create(candleStream)
        .then(() -> acceptHeartbeat(Duration.ofSeconds(0)))
        .then(() -> acceptMidPrice(Duration.ofSeconds(10), 15))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, false))        
        .then(() -> acceptMidPrice(Duration.ofSeconds(30), 17))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 17, 15, 17, false))
        .then(() -> acceptMidPrice(Duration.ofSeconds(65), 18))        
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 17, 15, 17, true))        
        .expectNext(buildCandle(Duration.ofMinutes(2), 17, 18, 17, 18, false))
        .then(this::completeSources)
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void testCreateFullCandleSingleValue() {

    StepVerifier.create(candleStream)
        .then(() -> acceptHeartbeat(Duration.ofSeconds(0)))
        .then(() -> acceptMidPrice(Duration.ofSeconds(10), 15))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, false))
        .then(() -> acceptHeartbeat(Duration.ofSeconds(65)))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, true))
        .expectNext(buildCandle(Duration.ofMinutes(2), 15, 15, 15, 15, false))
        .then(this::completeSources)
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void testCreateMultiCandleFromFrequentHeartbeatsOnly() {
    
    StepVerifier.create(candleStream)
        .then(() -> acceptHeartbeat(Duration.ofSeconds(0)))
        .then(() -> acceptMidPrice(Duration.ofSeconds(10), 15))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, false))
        .then(() -> acceptHeartbeat(Duration.ofSeconds(65)))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, true))
        .expectNext(buildCandle(Duration.ofMinutes(2), 15, 15, 15, 15, false))
        .then(() -> acceptHeartbeat(Duration.ofSeconds(125)))
        .expectNext(buildCandle(Duration.ofMinutes(2), 15, 15, 15, 15, true))
        .expectNext(buildCandle(Duration.ofMinutes(3), 15, 15, 15, 15, false))
        .then(() -> acceptHeartbeat(Duration.ofSeconds(185)))
        .expectNext(buildCandle(Duration.ofMinutes(3), 15, 15, 15, 15, true))
        .expectNext(buildCandle(Duration.ofMinutes(4), 15, 15, 15, 15, false))
        .then(this::completeSources)
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void testCreateMultiCandleFromInfrequentHeartbeatsOnly() {

    StepVerifier.create(candleStream)
        .then(() -> acceptHeartbeat(Duration.ofSeconds(0)))
        .then(() -> acceptMidPrice(Duration.ofSeconds(10), 15))        
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, false))
        .then(() -> acceptHeartbeat(Duration.ofSeconds(185)))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, true))
        .expectNext(buildCandle(Duration.ofMinutes(2), 15, 15, 15, 15, true))
        .expectNext(buildCandle(Duration.ofMinutes(3), 15, 15, 15, 15, true))
        .expectNext(buildCandle(Duration.ofMinutes(4), 15, 15, 15, 15, false))
        .then(this::completeSources)
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void testCreateMultiCandleFromPricesOnly() {
    acceptHeartbeat(Duration.ofSeconds(0));
    acceptMidPrice(Duration.ofSeconds(10), 15);
    acceptMidPrice(Duration.ofSeconds(30), 17);
    acceptMidPrice(Duration.ofSeconds(65), 18);
    acceptMidPrice(Duration.ofSeconds(75), 19);
    acceptMidPrice(Duration.ofSeconds(85), 20);
    acceptMidPrice(Duration.ofSeconds(125), 18);
    acceptMidPrice(Duration.ofSeconds(150), 16);
    acceptMidPrice(Duration.ofSeconds(165), 14);
    acceptMidPrice(Duration.ofSeconds(180), 12);
    acceptMidPrice(Duration.ofSeconds(185), 11);
    acceptMidPrice(Duration.ofSeconds(200), 13);

    StepVerifier.create(candleStream)
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 15, 15, 15, false))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 17, 15, 17, false))
        .expectNext(buildCandle(Duration.ofMinutes(1), 15, 17, 15, 17, true))
        .expectNext(buildCandle(Duration.ofMinutes(2), 17, 18, 17, 18, false))
        .expectNext(buildCandle(Duration.ofMinutes(2), 17, 19, 17, 19, false))
        .expectNext(buildCandle(Duration.ofMinutes(2), 17, 20, 17, 20, false))
        .expectNext(buildCandle(Duration.ofMinutes(2), 17, 20, 17, 20, true))
        .expectNext(buildCandle(Duration.ofMinutes(3), 20, 20, 18, 18, false))
        .expectNext(buildCandle(Duration.ofMinutes(3), 20, 20, 16, 16, false))
        .expectNext(buildCandle(Duration.ofMinutes(3), 20, 20, 14, 14, false))
        .expectNext(buildCandle(Duration.ofMinutes(3), 20, 20, 14, 14, true))
        .expectNext(buildCandle(Duration.ofMinutes(4), 14, 14, 12, 12, false))
        .expectNext(buildCandle(Duration.ofMinutes(4), 14, 14, 11, 11, false))
        .expectNext(buildCandle(Duration.ofMinutes(4), 14, 14, 11, 13, false))
        .then(this::completeSources)
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  private void acceptMidPrice(Duration offset, int mid) {
    orderBookSink.next(createSimpleBookWithMid(offset, mid));
  }

  private void acceptHeartbeat(Duration offset) {
    timeSink.next(START_TIME.plus(offset));
  }

  private OrderBook createSimpleBookWithMid(Duration offset, int mid) {
    return OrderBook.builder()
        .time(START_TIME.plus(offset))
        .bid(Quote.builder().price(BigDecimal.valueOf(mid - 1L)).volume(10L).build())
        .ask(Quote.builder().price(BigDecimal.valueOf(mid + 1L)).volume(10L).build())
        .build();
  }

  protected void completeSources() {
    timeSink.complete();
    orderBookSink.complete();
  }

  private Candle buildCandle(Duration offset, int o, int h, int l, int c, boolean complete) {
    return Candle.builder()
        .time(START_TIME.plus(offset))
        .open(BigDecimal.valueOf(o))
        .high(BigDecimal.valueOf(h))
        .low(BigDecimal.valueOf(l))
        .close(BigDecimal.valueOf(c))
        .complete(complete)
        .build();
  }
}
