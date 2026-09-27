package company.vk.edu.distrib.compute.nixxx11.urlshortener;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.nixxx11.urlshortener.http.AbstractHttpService;
import company.vk.edu.distrib.compute.nixxx11.urlshortener.http.ExceptionMappingHandler;
import company.vk.edu.distrib.compute.nixxx11.urlshortener.http.HttpUtils;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;

public class UrlShortenerServiceImpl extends AbstractHttpService implements UrlShortenerService {
  private static final int HTTP_UNPROCESSABLE_CONTENT = 422;
  private static final String ALLOWED_ID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  private static final int ID_LENGTH = 10;

  private final Dao<String> linksDao;
  private final String host;
  private final Random random = new Random();

  public UrlShortenerServiceImpl(
      final int port,
      final Dao<String> linksDao
  ) throws IOException {
    super(port);
    this.linksDao = linksDao;
    this.host = "http://localhost:" + port;
  }

  @Override
  protected Map<String, Map<String, HttpHandler>> getHandlers() {
    return Map.of(
        "/v0/status", Map.of("GET", new ExceptionMappingHandler(this::getStatus)),
        "/v0/links", Map.of(
            "POST", new ExceptionMappingHandler(this::createLink)
        ),
        "/v0/links/", Map.of(
            "GET", new ExceptionMappingHandler(this::getLink, Map.of(NoSuchElementException.class, HTTP_NOT_FOUND)),
            "PUT", new ExceptionMappingHandler(this::updateLink, Map.of(NoSuchElementException.class, HTTP_NOT_FOUND)),
            "DELETE", new ExceptionMappingHandler(this::deleteLink)
        ),
        "/", Map.of("GET", new ExceptionMappingHandler(this::getRedirect, Map.of(NoSuchElementException.class, HTTP_NOT_FOUND)))
    );
  }

  public void getStatus(final HttpExchange exchange) throws IOException {
    HttpUtils.writeResponse(exchange, HTTP_OK, "OK");
  }

  public void createLink(final HttpExchange exchange) throws IOException {
    final String link = HttpUtils.readRequest(exchange);
    if (!isValidLink(link)) {
      HttpUtils.writeResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
      return;
    }

    final String id = randomId();
    linksDao.upsert(id, link);

    HttpUtils.writeResponse(exchange, HTTP_CREATED, host + '/' + id);
  }

  public void getLink(final HttpExchange exchange) throws IOException {
    final String path = exchange.getRequestURI().getPath();
    final String key = path.substring("/v0/links/".length());
    if (!isValidId(key)) {
      HttpUtils.writeResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
      return;
    }

    final String value = linksDao.get(key);

    HttpUtils.writeResponse(exchange, HTTP_OK, value);
  }

  public void updateLink(final HttpExchange exchange) throws IOException {
    final String path = exchange.getRequestURI().getPath();
    final String key = path.substring("/v0/links/".length());
    if (!isValidId(key)) {
      HttpUtils.writeResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
      return;
    }

    final String value = linksDao.get(key);

    final String link = HttpUtils.readRequest(exchange);
    if (!link.equals(value)) {
      if (!isValidLink(link)) {
        HttpUtils.writeResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
        return;
      }
      linksDao.upsert(key, link);
    }

    HttpUtils.writeResponse(exchange, HTTP_OK);
  }

  public void deleteLink(final HttpExchange exchange) throws IOException {
    final String path = exchange.getRequestURI().getPath();
    final String key = path.substring("/v0/links/".length());
    if (!isValidId(key)) {
      HttpUtils.writeResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
      return;
    }

    linksDao.delete(key);

    HttpUtils.writeResponse(exchange, HTTP_ACCEPTED);
  }

  public void getRedirect(final HttpExchange exchange) throws IOException {
    final String path = exchange.getRequestURI().getPath();
    final String key = path.substring("/".length());
    if (!isValidId(key)) {
      HttpUtils.writeResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
      return;
    }

    final String value = linksDao.get(key);

    HttpUtils.redirect(exchange, value);
  }

  private String randomId() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ID_LENGTH; i++) {
      final char c = ALLOWED_ID_CHARS.charAt(random.nextInt(ALLOWED_ID_CHARS.length()));
      sb.append(c);
    }
    return sb.toString();
  }

  private boolean isValidId(final String id) {
    return id.length() == ID_LENGTH && id.chars().allMatch(c -> ALLOWED_ID_CHARS.indexOf(c) != -1);
  }

  private boolean isValidLink(final String link) {
    try {
      final URI uri = new URI(link);
      return uri.isAbsolute();
    } catch (URISyntaxException e) {
      return false;
    }
  }
}
