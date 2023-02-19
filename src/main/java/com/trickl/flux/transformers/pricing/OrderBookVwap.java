package com.trickl.flux.transformers.pricing;

import com.trickl.model.pricing.primitives.OrderBook;
import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrderBookVwap implements Function<OrderBook, Optional<BigDecimal>> {    
  private static final Function<OrderBook, Optional<BigDecimal>> MID = new OrderBookMid();
   
  @Override
  public Optional<BigDecimal> apply(OrderBook orderBook) {
    BigDecimal priceVolumeSum = BigDecimal.ZERO;
    BigDecimal volumeSum = BigDecimal.ZERO;
    List<Quote> quotes =
        Stream.concat(
            // Best last
            orderBook.getBids().stream(), 
            orderBook.getAsks().stream())
            .collect(Collectors.toList());

    for (Quote quote : quotes) {
      BigDecimal price = quote.getPrice();
      Long volume = quote.getVolume();
      if (volume != null) {
        BigDecimal decimalVolume = BigDecimal.valueOf(quote.getVolume());
        priceVolumeSum = priceVolumeSum.add(price.multiply(decimalVolume, MathContext.DECIMAL64),
            MathContext.DECIMAL64);
        volumeSum = volumeSum.add(decimalVolume);
      }
    }

    if (BigDecimal.ZERO.equals(volumeSum)) {
      return MID.apply(orderBook);
    }

    return Optional.of(priceVolumeSum.divide(volumeSum, MathContext.DECIMAL64));
  }    
}
