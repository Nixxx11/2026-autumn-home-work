package company.vk.edu.distrib.compute.nixxx11.urlshortener.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.Request;

import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

public abstract class AbstractHandler implements HttpHandler {
  private static final Response BAD_METHOD_RESPONSE = new Response.Empty(HTTP_BAD_METHOD);
  private static final Response INTERNAL_ERROR_RESPONSE = new Response.Empty(HTTP_INTERNAL_ERROR);

  protected Response handleGet(final Request request) throws IOException {
    return BAD_METHOD_RESPONSE;
  }

  protected Response handlePost(final Request request, final String content) throws IOException {
    return BAD_METHOD_RESPONSE;
  }

  protected Response handlePut(final Request request, final String content) throws IOException {
    return BAD_METHOD_RESPONSE;
  }

  protected Response handleDelete(final Request request) throws IOException {
    return BAD_METHOD_RESPONSE;
  }

  @Override
  public void handle(final HttpExchange exchange) throws IOException {
    try (exchange) {
      Response response;
      try {
        response = switch (exchange.getRequestMethod()) {
          case "GET" -> handleGet(exchange);
          case "POST" -> handlePost(exchange, readBody(exchange));
          case "PUT" -> handlePut(exchange, readBody(exchange));
          case "DELETE" -> handleDelete(exchange);
          default -> BAD_METHOD_RESPONSE;
        };
      } catch (final Exception e) {
        response = INTERNAL_ERROR_RESPONSE;
      }
      response.handle(exchange);
    }
  }

  private static String readBody(final HttpExchange exchange) throws IOException {
    final byte[] bytes = exchange.getRequestBody().readAllBytes();
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
