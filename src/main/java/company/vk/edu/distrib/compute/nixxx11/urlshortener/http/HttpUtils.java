package company.vk.edu.distrib.compute.nixxx11.urlshortener.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nullable;

import com.sun.net.httpserver.HttpExchange;

import static java.net.HttpURLConnection.HTTP_MOVED_PERM;

public final class HttpUtils {
  private HttpUtils() {}

  public static String readRequest(final HttpExchange exchange) throws IOException {
    final byte[] bytes = exchange.getRequestBody().readAllBytes();
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static void writeResponse(
      final HttpExchange exchange,
      final int status
  ) throws IOException {
    writeResponse(exchange, status, null);
  }

  public static void writeResponse(
      final HttpExchange exchange,
      final int status,
      final @Nullable String response
  ) throws IOException {
    if (response == null) {
      exchange.sendResponseHeaders(status, 0);
      return;
    }
    final byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("content-type", "text/html; charset=utf-8");
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
  }

  public static void redirect(
      final HttpExchange exchange,
      final String url
  ) throws IOException {
    exchange.getResponseHeaders().add("location", url);
    exchange.sendResponseHeaders(HTTP_MOVED_PERM, 0);
  }
}
