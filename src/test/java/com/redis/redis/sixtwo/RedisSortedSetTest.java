package com.redis.redis.sixtwo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
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
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.testcontainers.RedisModulesContainer;

import io.lettuce.core.ClientOptions;

@Testcontainers
@SpringBootTest(classes = RedisSortedSetTest.Config.class)
class RedisSortedSetTest {

  @Container
  static final RedisModulesContainer REDIS = new RedisModulesContainer();

  @Resource(name = "stringRedisTemplate")
  private ZSetOperations<String, String> zSetOps;

  // - Add ZMSCORE command that returns an array of scores (#7593)
  // - Add ZDIFF and ZDIFFSTORE commands (#7961)
  // - Add ZINTER and ZUNION commands (#7794)
  // - Add ZRANDMEMBER command (#8297)
  // - Add the REV, BYLEX and BYSCORE arguments to ZRANGE, and the ZRANGESTORE
  // command

  @Test
  void testZMSCORE() {
    // ZADD myzset 1 "one"
    // ZADD myzset 2 "two"
    // ZMSCORE myzset "one" "two" "nofield"
    // 1) "1"
    // 2) "2"
    // 3) (nil)
    zSetOps.add("myzset", "one", 1);
    zSetOps.add("myzset", "two", 2);
    List<Double> scores = zSetOps.score("myzset", "one", "two", "nofield");

    assertArrayEquals(new Double[] { 1.0, 2.0, null }, scores.toArray());
  }

  @Test
  void testZDIFF() {
    // ZADD zset1 1 "one"
    // ZADD zset1 2 "two"
    // ZADD zset1 3 "three"
    // ZADD zset2 1 "one"
    // ZADD zset2 2 "two"
    // ZDIFF 2 zset1 zset2
    // 1) "three"
    // redis> ZDIFF 2 zset1 zset2 WITHSCORES
    // 1) "three"
    // 2) "3"
    zSetOps.add("zset1", "one", 1);
    zSetOps.add("zset1", "two", 2);
    zSetOps.add("zset1", "three", 3);

    zSetOps.add("zset2", "one", 1);
    zSetOps.add("zset2", "two", 2);

    Set<String> diffs = zSetOps.difference("zset1", "zset2");
    assertArrayEquals(new String[] { "three" }, diffs.toArray());

    Set<TypedTuple<String>> diffsWScores = zSetOps.differenceWithScores("zset1", "zset2");
    assertEquals(1, diffsWScores.size());
    TypedTuple<String> dtt = diffsWScores.iterator().next();
    assertEquals("three", dtt.getValue());
    assertEquals(3.0, dtt.getScore());
  }

  @Test
  void testZDIFFSTORE() {
    // ZADD zset1 1 "one"
    // ZADD zset1 2 "two"
    // ZADD zset1 3 "three"
    // ZADD zset2 1 "one"
    // ZADD zset2 2 "two"
    // redis> ZDIFFSTORE out 2 zset1 zset2
    // redis> ZRANGE out 0 -1 WITHSCORES
    // 1) "three"
    // 2) "3"
    // redis>
    zSetOps.add("zset1", "one", 1);
    zSetOps.add("zset1", "two", 2);
    zSetOps.add("zset1", "three", 3);

    zSetOps.add("zset2", "one", 1);
    zSetOps.add("zset2", "two", 2);

    zSetOps.differenceAndStore("zset1", List.of("zset2"), "out");
    Set<TypedTuple<String>> withScores = zSetOps.rangeWithScores("out", 0, -1);
    assertEquals(1, withScores.size());
    TypedTuple<String> dtt = withScores.iterator().next();
    assertEquals("three", dtt.getValue());
    assertEquals(3.0, dtt.getScore());
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
