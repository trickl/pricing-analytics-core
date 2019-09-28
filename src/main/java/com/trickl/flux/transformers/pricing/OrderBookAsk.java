package com.trickl.flux.transformers.pricing;

import com.trickl.model.pricing.primitives.OrderBook;
import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

public class OrderBookAsk implements Function<OrderBook, Optional<BigDecimal>> {
    
  private static final Comparator<Quote> BEST_ASK = new QuoteBestAskComparator();

  @Override
  public Optional<BigDecimal> apply(OrderBook orderBook) {
    return orderBook.getBids().stream().sorted(BEST_ASK).limit(1).map(q -> q.getPrice()).findAny();
  }    
}
