package com.trickl.flux.transformers.pricing;

import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Optional;

public class QuoteBestAskComparator implements Comparator<Quote> {
  @Override
  public int compare(Quote a, Quote b) {
    return Optional.ofNullable(a.getPrice())
        .orElse(BigDecimal.valueOf(Long.MAX_VALUE))
        .compareTo(Optional.ofNullable(b.getPrice()).orElse(BigDecimal.valueOf(Long.MAX_VALUE)));
  }
}
