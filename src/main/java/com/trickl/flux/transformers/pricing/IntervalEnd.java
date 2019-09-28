package com.trickl.flux.transformers.pricing;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

@Log
@RequiredArgsConstructor
public class IntervalEnd implements Function<Instant, Instant> {

  private final Duration intervalWidth;

  public Instant apply(Instant time) {
    return Instant.ofEpochMilli(
        ((time.toEpochMilli() / intervalWidth.toMillis()) + 1) * intervalWidth.toMillis());
  }
}
