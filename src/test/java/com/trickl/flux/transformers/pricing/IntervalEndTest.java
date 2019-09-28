package com.trickl.flux.transformers.pricing;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class IntervalEndTest {

  private static final Instant START_TIME = Instant.parse("2019-01-01T12:00:00.00Z");

  private IntervalEnd midTransformer;

  @Before
  public void setup() {
    midTransformer = new IntervalEnd(Duration.ofMinutes(1));
  }

  @Test
  public void testCandleEnd() {
    assertThat(midTransformer.apply(START_TIME), is(START_TIME.plus(Duration.ofMinutes(1))));
    assertThat(
        midTransformer.apply(START_TIME.plus(Duration.ofSeconds(10))),
        is(START_TIME.plus(Duration.ofMinutes(1))));
    assertThat(
        midTransformer.apply(START_TIME.plus(Duration.ofSeconds(30))),
        is(START_TIME.plus(Duration.ofMinutes(1))));
    assertThat(
        midTransformer.apply(START_TIME.plus(Duration.ofSeconds(59))),
        is(START_TIME.plus(Duration.ofMinutes(1))));
    assertThat(
        midTransformer.apply(START_TIME.plus(Duration.ofSeconds(60))),
        is(START_TIME.plus(Duration.ofMinutes(2))));
    assertThat(
        midTransformer.apply(START_TIME.plus(Duration.ofSeconds(61))),
        is(START_TIME.plus(Duration.ofMinutes(2))));
    assertThat(
        midTransformer.apply(START_TIME.plus(Duration.ofSeconds(119))),
        is(START_TIME.plus(Duration.ofMinutes(2))));
    assertThat(
        midTransformer.apply(START_TIME.plus(Duration.ofSeconds(120))),
        is(START_TIME.plus(Duration.ofMinutes(3))));
  }
}
