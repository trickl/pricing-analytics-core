package com.trickl.flux.transformers.pricing;

import com.trickl.model.pricing.primitives.OrderBook;
import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrderBookMid implements Function<OrderBook, Optional<BigDecimal>> {
  private static final BigDecimal TWO = BigDecimal.valueOf(2L);
  private static final Comparator<Quote> BEST_BID = new QuoteBestBidComparator();
  private static final Comparator<Quote> BEST_ASK = new QuoteBestAskComparator();
    
  @Override
  public Optional<BigDecimal> apply(OrderBook orderBook) {
    BigDecimal mid = BigDecimal.ZERO;    
    List<Quote> quotes =
        Stream.concat(
            // Best last
            orderBook.getBids().stream().sorted(BEST_BID).limit(1), 
            orderBook.getAsks().stream().sorted(BEST_ASK).limit(1))
            .collect(Collectors.toList());
    
    if (quotes.isEmpty()) {
      return Optional.empty();
    }

    for (Quote quote : quotes) {
      BigDecimal quotePrice = quote.getPrice();
      mid = mid.add(quotePrice, MathContext.DECIMAL64);      
    }
    
    if (quotes.size() == 1) {
      return Optional.of(mid);
    }

    return Optional.of(mid.divide(TWO, MathContext.DECIMAL64));
  }    
}
