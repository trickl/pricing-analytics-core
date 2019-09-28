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

public class OrderBookVwap implements Function<OrderBook, Optional<BigDecimal>> {
    
  private static final Comparator<Quote> BEST_BID = new QuoteBestBidComparator();
  private static final Comparator<Quote> BEST_ASK = new QuoteBestAskComparator();
  private static final Function<OrderBook, Optional<BigDecimal>> MID = new OrderBookMid();
   
  @Override
  public Optional<BigDecimal> apply(OrderBook orderBook) {
    BigDecimal vwap = BigDecimal.ZERO;
    BigDecimal totalVolume = BigDecimal.ZERO;
    List<Quote> quotes =
        Stream.concat(
            // Best last
            orderBook.getBids().stream().sorted(BEST_BID).limit(1), 
            orderBook.getAsks().stream().sorted(BEST_ASK).limit(1))
            .collect(Collectors.toList());

    for (Quote quote : quotes) {
      BigDecimal price = quote.getPrice();
      Long volume = quote.getVolume();
      if (volume != null) {
        BigDecimal decimalVolume = BigDecimal.valueOf(quote.getVolume());
        vwap = vwap.add(price.multiply(decimalVolume, MathContext.DECIMAL64),
            MathContext.DECIMAL64);
        totalVolume = totalVolume.add(decimalVolume);
      }
    }

    if (BigDecimal.ZERO.equals(totalVolume)) {
      return MID.apply(orderBook);
    }

    return Optional.of(vwap.divide(totalVolume, MathContext.DECIMAL64));
  }    
}
