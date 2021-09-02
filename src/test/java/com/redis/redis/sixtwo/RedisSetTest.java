package com.redis.redis.sixtwo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  void testSimpleExample() {
    // redis> SADD colors red yellow green fushia
    // redis> SADD colors yellow
    // redis> SMEMBERS colors
    // redis> SISMEMBER colors green
    // redis> SISMEMBER colors magenta
    // redis> SREM colors green
    // redis> SREM colors green
    // redis> SMEMBERS colors
    
    setOps.add("colors", "red", "yellow", "green", "fushia");
    setOps.add("colors", "yellow");
    Set<String> members = setOps.members("colors");
    assertTrue(members.containsAll(List.of("red", "yellow", "green", "fushia")));
    assertTrue(setOps.isMember("colors", "green"));
    assertFalse(setOps.isMember("colors", "magenta"));
    assertEquals(1, setOps.remove("colors", "green"));
    members = setOps.members("colors");
    assertTrue(members.containsAll(List.of("red", "yellow", "fushia")));
  }

  @Test
  void testSMISMEMBER() {
    // redis> SADD colors red yellow green fushia
    // redis> SMISMEMBER colors red black green
    // 1) (integer) 1
    // 2) (integer) 0
    // 3) (integer) 1
    
    setOps.add("colors", "red", "yellow", "green", "fushia");
    Map<Object, Boolean> memberCheck = setOps.isMember("colors", "red", "black", "green");
    assertTrue(memberCheck.get("red"));
    assertFalse(memberCheck.get("black"));
    assertTrue(memberCheck.get("green"));
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
