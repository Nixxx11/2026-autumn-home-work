package company.vk.edu.distrib.compute.nixxx11.urlshortener.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.HttpService;

import static java.net.HttpURLConnection.HTTP_BAD_METHOD;

public abstract class AbstractHttpService implements HttpService {
  private final HttpServer httpServer;

  public AbstractHttpService(final int port) throws IOException {
    this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    final Map<String, Map<String, HttpHandler>> handlers = getHandlers();
    handlers.forEach(this::setupHandler);
  }

  protected abstract Map<String, Map<String, HttpHandler>> getHandlers();

  private void setupHandler(final String path, final Map<String, HttpHandler> methods) {
    httpServer.createContext(
        path,
        exchange -> {
          try (exchange) {
            final HttpHandler handler = methods.get(exchange.getRequestMethod());
            if (handler != null) {
              handler.handle(exchange);
            } else {
              exchange.sendResponseHeaders(HTTP_BAD_METHOD, 0);
            }
          }
        }
    );
  }

  @Override
  public void start() {
    httpServer.start();
  }

  @Override
  public void stop() {
    httpServer.stop(1);
  }
}
