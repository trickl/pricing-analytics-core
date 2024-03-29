package com.trickl.flux.transformers.pricing;

import static org.junit.Assert.assertEquals;

import com.trickl.model.pricing.primitives.OrderBook;
import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class OrderBookVwapTest {

  private OrderBookVwap vwap;

  @Before
  public void setup() {
    vwap = new OrderBookVwap();
  }

  @Test
  public void testHandlesMissingVolumeOneSide() {
    OrderBook book =
        OrderBook.builder()
            .time(Instant.parse("2007-12-03T10:15:30.00Z"))
            .bid(Quote.builder().price(BigDecimal.valueOf(10)).build())
            .ask(Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build())
            .build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(true, result.isPresent());
    assertEquals(BigDecimal.valueOf(20), result.get());
  }

  @Test
  public void testHandlesMissingVolumeBothSides() {
    OrderBook book =
        OrderBook.builder()
            .time(Instant.parse("2007-12-03T10:15:30.00Z"))
            .bid(Quote.builder().price(BigDecimal.valueOf(10)).build())
            .ask(Quote.builder().price(BigDecimal.valueOf(20)).build())
            .build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(true, result.isPresent());
    assertEquals(BigDecimal.valueOf(15), result.get());
  }

  @Test
  public void testHandlesZeroVolumeOneSide() {
    OrderBook book =
        OrderBook.builder()
            .time(Instant.parse("2007-12-03T10:15:30.00Z"))
            .bid(Quote.builder().price(BigDecimal.valueOf(10)).volume(0L).build())
            .ask(Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build())
            .build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(true, result.isPresent());
    assertEquals(BigDecimal.valueOf(20), result.get());
  }

  @Test
  public void testHandlesZeroVolumeBothSides() {
    OrderBook book =
        OrderBook.builder()
            .time(Instant.parse("2007-12-03T10:15:30.00Z"))
            .bid(Quote.builder().price(BigDecimal.valueOf(10)).volume(0L).build())
            .ask(Quote.builder().price(BigDecimal.valueOf(20)).volume(0L).build())
            .build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(true, result.isPresent());
    assertEquals(BigDecimal.valueOf(15), result.get());
  }

  @Test
  public void testHandlesMissingBids() {
    OrderBook book =
        OrderBook.builder()
            .time(Instant.parse("2007-12-03T10:15:30.00Z"))
            .ask(Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build())
            .build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(true, result.isPresent());
    assertEquals(BigDecimal.valueOf(20), result.get());
  }

  @Test
  public void testHandlesMissingAsks() {
    OrderBook book =
        OrderBook.builder()
            .time(Instant.parse("2007-12-03T10:15:30.00Z"))
            .bid(Quote.builder().price(BigDecimal.valueOf(10)).volume(100L).build())
            .build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(true, result.isPresent());
    assertEquals(BigDecimal.valueOf(10), result.get());
  }

  @Test
  public void testHandlesEmptyBook() {
    OrderBook book = OrderBook.builder().time(Instant.parse("2007-12-03T10:15:30.00Z")).build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(false, result.isPresent());
  }

  @Test
  public void testSimpleFlatBookHasCorrectVwap() {
    OrderBook book =
        OrderBook.builder()
            .time(Instant.parse("2007-12-03T10:15:30.00Z"))
            .bid(Quote.builder().price(BigDecimal.valueOf(10)).volume(100L).build())
            .ask(Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build())
            .build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(true, result.isPresent());
    assertEquals(BigDecimal.valueOf(17.5), result.get());
  }

  @Test
  public void testBookWithDepthHasCorrectVwap() {
    OrderBook book =
        OrderBook.builder()
            .time(Instant.parse("2007-12-03T10:15:30.00Z"))
            .bid(Quote.builder().price(BigDecimal.valueOf(5)).volume(200L).build())
            .bid(Quote.builder().price(BigDecimal.valueOf(10)).volume(100L).build())
            .ask(Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build())
            .ask(Quote.builder().price(BigDecimal.valueOf(50)).volume(500L).build())
            .build();

    Optional<BigDecimal> result = vwap.apply(book);
    assertEquals(true, result.isPresent());
    assertEquals(BigDecimal.valueOf(30), result.get());
  }
}
