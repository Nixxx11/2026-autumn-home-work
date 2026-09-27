package company.vk.edu.distrib.compute.nixxx11.urlshortener;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.Request;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.nixxx11.urlshortener.http.AbstractService;
import company.vk.edu.distrib.compute.nixxx11.urlshortener.http.AbstractHandler;
import company.vk.edu.distrib.compute.nixxx11.urlshortener.http.Response;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;

public class UrlShortenerServiceImpl extends AbstractService implements UrlShortenerService {
  private static final int HTTP_UNPROCESSABLE_CONTENT = 422;

  private static final String ALLOWED_ID_CHARS;
  static {
    final StringBuilder sb = new StringBuilder();
    for (char c = 'a'; c <= 'z'; c++) {
      sb.append(c);
    }
    for (char c = 'A'; c <= 'Z'; c++) {
      sb.append(c);
    }
    for (char c = '0'; c <= '9'; c++) {
      sb.append(c);
    }
    ALLOWED_ID_CHARS = sb.toString();
  }
  private static final int ID_LENGTH = 10;
  private static final Pattern ID_PATTERN = Pattern.compile("[" + ALLOWED_ID_CHARS + "]{" + ID_LENGTH + "}");

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
  protected Map<String, HttpHandler> getHandlers() {
    return Map.of(
        "/v0/status", new StatusHandler(),
        "/v0/links", new CreateLinksHandler(),
        "/v0/links/", new LinksHandler(),
        "/", new RedirectHandler()
    );
  }

  private static class StatusHandler extends AbstractHandler {
    @Override
    protected Response handleGet(final Request request) {
      return new Response.Basic(HTTP_OK, "OK");
    }
  }

  private class CreateLinksHandler extends AbstractHandler {
    @Override
    protected Response handlePost(final Request request, final String content) throws IOException {
      if (!isValidLink(content)) {
        return new Response.Basic(HTTP_UNPROCESSABLE_CONTENT, "Invalid link: " + content);
      }

      final String id = randomId();
      linksDao.upsert(id, content);

      return new Response.Basic(HTTP_CREATED, host + '/' + id);
    }
  }

  private class LinksHandler extends AbstractHandler {
    @Override
    protected Response handleGet(final Request request) throws IOException {
      final String id = getId(request);
      if (!isValidId(id)) {
        return new Response.Basic(HTTP_UNPROCESSABLE_CONTENT, "Invalid id: " + id);
      }

      final String value;
      try {
        value = linksDao.get(id);
      } catch (NoSuchElementException e) {
        return new Response.Basic(HTTP_NOT_FOUND, "No such id: " + id);
      }

      return new Response.Basic(HTTP_OK, value);
    }

    @Override
    protected Response handlePut(final Request request, final String content) throws IOException {
      final String id = getId(request);
      if (!isValidId(id)) {
        return new Response.Basic(HTTP_UNPROCESSABLE_CONTENT, "Invalid id: " + id);
      }

      if (!isValidLink(content)) {
        return new Response.Basic(HTTP_UNPROCESSABLE_CONTENT, "Invalid link: " + content);
      }

      final String value;
      try {
        value = linksDao.get(id);
      } catch (NoSuchElementException e) {
        return new Response.Basic(HTTP_NOT_FOUND, "No such id: " + id);
      }
      if (!content.equals(value)) {
        linksDao.upsert(id, content);
      }

      return new Response.Empty(HTTP_OK);
    }

    @Override
    protected Response handleDelete(final Request request) throws IOException {
      final String id = getId(request);
      if (!isValidId(id)) {
        return new Response.Basic(HTTP_UNPROCESSABLE_CONTENT, "Invalid id: " + id);
      }

      linksDao.delete(id);

      return new Response.Empty(HTTP_ACCEPTED);
    }

    private static String getId(final Request request) {
      final String path = request.getRequestURI().getPath();
      return path.substring("/v0/links/".length());
    }
  }

  private class RedirectHandler extends AbstractHandler {
    @Override
    protected Response handleGet(final Request request) throws IOException {
      final String id = getId(request);
      if (!isValidId(id)) {
        return new Response.Basic(HTTP_UNPROCESSABLE_CONTENT, "Invalid id: " + id);
      }

      final String value;
      try {
        value = linksDao.get(id);
      } catch (NoSuchElementException e) {
        return new Response.Basic(HTTP_NOT_FOUND, "No such id: " + id);
      }

      return new Response.Redirect(value);
    }

    private static String getId(final Request request) {
      final String path = request.getRequestURI().getPath();
      return path.substring("/".length());
    }
  }

  private String randomId() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ID_LENGTH; i++) {
      final int pos = random.nextInt(ALLOWED_ID_CHARS.length());
      final char c = ALLOWED_ID_CHARS.charAt(pos);
      sb.append(c);
    }
    return sb.toString();
  }

  private static boolean isValidId(final String id) {
    return ID_PATTERN.matcher(id).matches();
  }

  private static boolean isValidLink(final String link) {
    try {
      final URI uri = new URI(link);
      return uri.isAbsolute();
    } catch (URISyntaxException e) {
      return false;
    }
  }
}
