package dev.restate.integration.e2egreeter;

import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import java.util.concurrent.CountDownLatch;

public final class Main {

  public static void main(String[] args) throws InterruptedException {
    RestateHttpServer.listen(Endpoint.bind(new Greeter()), 9080);
    // The Vert.x server runs on its own threads; block the main thread so the container stays up.
    new CountDownLatch(1).await();
  }
}
