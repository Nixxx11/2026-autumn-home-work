package company.vk.edu.distrib.compute.nixxx11.kv.dao;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import company.vk.edu.distrib.compute.Dao;

public class ClusterRemoteDao implements Dao<String> {
  private final List<RemoteDao> nodes;

  public ClusterRemoteDao(final Set<URI> baseUris) {
    this.nodes = new ArrayList<>(baseUris.size());
    try {
      for (final URI uri : baseUris) {
        nodes.add(new RemoteDao(uri));
      }
    } catch (final RuntimeException e) {
      close();
      throw e;
    }
  }

  @Override
  public String get(final String key) throws NoSuchElementException, IllegalArgumentException, IOException {
    final RemoteDao dao = chooseNode(key);
    return dao.get(key);
  }

  @Override
  public void upsert(final String key, final String value) throws IllegalArgumentException, IOException {
    final RemoteDao dao = chooseNode(key);
    dao.upsert(key, value);
  }

  @Override
  public void delete(final String key) throws IllegalArgumentException, IOException {
    final RemoteDao dao = chooseNode(key);
    dao.delete(key);
  }

  private RemoteDao chooseNode(final String key) {
    RemoteDao chosenNode = nodes.get(0);
    int maxWeight = rendezvousWeight(key, chosenNode.getBaseUri());
    for (int i = 1; i < nodes.size(); i++) {
      final RemoteDao node = nodes.get(i);
      final int weight = rendezvousWeight(key, node.getBaseUri());
      if (weight > maxWeight
          || (weight == maxWeight && chosenNode.getBaseUri().compareTo(node.getBaseUri()) > 0)) {
        maxWeight = weight;
        chosenNode = node;
      }
    }
    return chosenNode;
  }

  private static int rendezvousWeight(final String key, final URI node) {
    return key.hashCode() ^ node.hashCode();
  }

  @Override
  public void close() {
    for (final RemoteDao dao : nodes) {
      dao.close();
    }
  }
}
