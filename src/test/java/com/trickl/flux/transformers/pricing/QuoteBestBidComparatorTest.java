package com.trickl.flux.transformers.pricing;

import static org.junit.Assert.*;

import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import org.junit.Before;
import org.junit.Test;

public class QuoteBestBidComparatorTest {

  private QuoteBestBidComparator comparator;

  @Before
  public void setup() {
    comparator = new QuoteBestBidComparator();
  }

  @Test
  public void testCompareFirstBetter() {
    Quote a = Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build();
    Quote b = Quote.builder().price(BigDecimal.valueOf(10)).volume(300L).build();
    int result = comparator.compare(a, b);
    assertEquals(-1, result);
  }

  @Test
  public void testCompareSecondBetter() {
    Quote a = Quote.builder().price(BigDecimal.valueOf(10)).volume(300L).build();
    Quote b = Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build();
    int result = comparator.compare(a, b);
    assertEquals(1, result);
  }

  @Test
  public void testCompareFirstNullPrice() {
    Quote a = Quote.builder().volume(300L).build();
    Quote b = Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build();
    int result = comparator.compare(a, b);
    assertEquals(1, result);
  }

  @Test
  public void testCompareSecondNullPrice() {
    Quote a = Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build();
    Quote b = Quote.builder().volume(300L).build();
    int result = comparator.compare(a, b);
    assertEquals(-1, result);
  }

  @Test
  public void testCompareEqual() {
    Quote a = Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build();
    Quote b = Quote.builder().price(BigDecimal.valueOf(20)).volume(300L).build();
    int result = comparator.compare(a, b);
    assertEquals(0, result);
  }

  @Test
  public void testCompareEqualBothNull() {
    Quote a = Quote.builder().volume(300L).build();
    Quote b = Quote.builder().volume(300L).build();
    int result = comparator.compare(a, b);
    assertEquals(0, result);
  }
}
