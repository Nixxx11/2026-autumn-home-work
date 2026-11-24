package company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation;

import java.io.IOException;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

@ModerationServiceFactoryTest
public class ModerationServiceFactoryImpl implements ModerationServiceFactory {
  @Override
  public ModerationService create(final String bootstrapServers) throws IOException {
    return new ModerationServiceImpl(bootstrapServers);
  }
}
