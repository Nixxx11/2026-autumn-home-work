package company.vk.edu.distrib.compute.nixxx11.urlshortener.http;

import java.io.IOException;
import java.util.Random;

import com.sun.net.httpserver.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogWrapper implements SimpleHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogWrapper.class);
  private static final Random RANDOM = new Random();

  private final SimpleHandler handler;

  public LogWrapper(final SimpleHandler handler) {
    this.handler = handler;
  }

  @Override
  public Response handle(final Request request, final String content) throws IOException {
    final int requestId = RANDOM.nextInt();
    if (content.isEmpty()) {
      LOG.info(
          "Got request '{} {}' (id={})",
          request.getRequestMethod(),
          request.getRequestURI().getPath(),
          requestId
      );
    } else {
      LOG.info(
          "Got request '{} {}' with body '{}' (id={})",
          request.getRequestMethod(),
          request.getRequestURI().getPath(),
          content,
          requestId
      );
    }
    final Response response = handler.handle(request, content);
    LOG.info(
        "Finished request with status {} (id={})",
        response.status(),
        requestId
    );
    return response;
  }
}
