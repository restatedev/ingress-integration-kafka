package dev.restate.integration.e2egreeter;

import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Raw;
import dev.restate.sdk.annotation.Service;
import java.nio.charset.StandardCharsets;

/**
 * The invocation target for the end-to-end test. Its single handler just prints what it received to
 * stdout with a marker; the test scrapes this container's logs to prove the Kafka record made it all
 * the way through Restate to a real handler.
 *
 * <p>Input is {@code @Raw byte[]} because the Kafka integration sends the record payload as raw bytes
 * with no content-type.
 */
@Service
public class Greeter {

  @Handler
  public void greet(Context ctx, @Raw byte[] payload) {
    System.out.println("GREETER_RECEIVED:" + new String(payload, StandardCharsets.UTF_8));
  }
}
