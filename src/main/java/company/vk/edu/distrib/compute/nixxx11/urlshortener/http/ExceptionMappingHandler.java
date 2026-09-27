package company.vk.edu.distrib.compute.nixxx11.urlshortener.http;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jspecify.annotations.Nullable;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

public class ExceptionMappingHandler implements HttpHandler {
  private final HttpHandler delegate;
  private final Function<Exception, @Nullable Integer> exceptionMapper;

  public ExceptionMappingHandler(
      final HttpHandler delegate,
      final Function<Exception, @Nullable Integer> exceptionMapper
  ) {
    this.delegate = delegate;
    this.exceptionMapper = exceptionMapper;
  }

  public ExceptionMappingHandler(
      final HttpHandler delegate,
      final Map<Class<? extends Exception>, Integer> exceptionMap
  ) {
    this(
        delegate,
        exception -> exceptionMap.get(exception.getClass())
    );
  }

  public ExceptionMappingHandler(final HttpHandler delegate) {
    this(
        delegate,
        Map.of()
    );
  }

  @Override
  public void handle(final HttpExchange exchange) throws IOException {
    try {
      delegate.handle(exchange);
    } catch (Exception e) {
      final Integer status = exceptionMapper.apply(e);
      if (status != null) {
        HttpUtils.writeResponse(exchange, status, e.getMessage());
      } else {
        HttpUtils.writeResponse(exchange, HTTP_INTERNAL_ERROR);
      }
    }
  }
}
