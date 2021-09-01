package com.redis.redis.sixtwo;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.SetOperations;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.testcontainers.RedisModulesContainer;

import io.lettuce.core.ClientOptions;

@Testcontainers
@SpringBootTest(classes = RedisSetTest.Config.class)
class RedisSetTest {

  @Container
  static final RedisModulesContainer REDIS = new RedisModulesContainer();

  @Resource(name = "stringRedisTemplate")
  private SetOperations<String, String> setOps;

  // - Add SMISMEMBER command that checks multiple members (#7615)

  @Test
  void testSMISMEMBER() {
    // redis> SADD myset "one"
    // redis> SADD myset "one"
    // redis> SMISMEMBER myset "one" "notamember"
    // 1) (integer) 1
    // 2) (integer) 0
    setOps.add("myset", "one");
    setOps.add("myset", "one");
    Map<Object, Boolean> memberCheck = setOps.isMember("myset", "one", "notamember");
    assertTrue(memberCheck.get("one"));
    assertFalse(memberCheck.get("notamember"));
  }

  @SpringBootApplication
  @Configuration
  static class Config {
    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {

      return new LettuceConnectionFactory(
          new RedisStandaloneConfiguration(REDIS.getContainerIpAddress(), REDIS.getMappedPort(6379)));
    }

    @Bean
    public LettuceClientConfigurationBuilderCustomizer defaultLettuceClientConfigurationBuilderCustomizer() {
      return clientConfigurationBuilder -> clientConfigurationBuilder
          .clientOptions(
              ClientOptions.builder().disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS).build())
          .shutdownTimeout(Duration.ofSeconds(2));
    }
  }

}
