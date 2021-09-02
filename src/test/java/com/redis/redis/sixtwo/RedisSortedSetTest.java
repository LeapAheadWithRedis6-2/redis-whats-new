package com.redis.redis.sixtwo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
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
  void testSimpleExample() {
    //    redis> ZADD game1 100 "Frank" 740 "Jennifer" 200 "Pieter" 512 "Dave" 690 "Ana"
    //    redis> ZADD game2 212 "Joe" 230 "Jennifer" 450 "Mary" 730 "Tom" 512 "Dave" 200 "Frank"
    //
    //    redis> ZRANGE game1 0 -1
    //    redis> ZRANGE game2 0 -1 WITHSCORES
    //
    //    redis> ZINTER 2 game1 game2 WITHSCORES
    //    redis> ZINTER 2 game1 game2 WITHSCORES AGGREGATE max
    //
    //    redis> ZDIFF 2 game1 game2 WITHSCORES
    //    redis> ZDIFFSTORE only_game_1 2 game1 game2
    //    redis> ZRANGE only_game_1 0 -1 WITHSCORES
    
    Set<TypedTuple<String>> game1 = Set.of( //
        TypedTuple.of("Frank", 100.0), TypedTuple.of("Jennifer", 740.0), 
        TypedTuple.of("Pieter", 200.0), TypedTuple.of("Dave", 512.0), 
        TypedTuple.of("Ana", 690.0));

    Set<TypedTuple<String>> game2 = Set.of( //
        TypedTuple.of("Joe", 212.0), TypedTuple.of("Jennifer", 230.0), 
        TypedTuple.of("Mary", 450.0), TypedTuple.of("Tom", 730.0), 
        TypedTuple.of("Dave", 512.0), TypedTuple.of("Frank", 200.0));  
    
    zSetOps.add("game1", game1);
    zSetOps.add("game2", game2);
    
    Set<String> game1Players = zSetOps.range("game1", 0, -1);
    assertArrayEquals(new String[] { "Frank", "Pieter", "Dave", "Ana", "Jennifer"}, game1Players.toArray());
    
    Set<TypedTuple<String>> game2PlayersWithScores = zSetOps.rangeWithScores("game2", 0, -1);
    TypedTuple<String> frankInGame2 = game2PlayersWithScores.iterator().next();
    assertEquals("Frank", frankInGame2.getValue());
    assertEquals(200.0, frankInGame2.getScore());
    
    Set<TypedTuple<String>> inBothGames = zSetOps.intersectWithScores("game1", "game2");
    TypedTuple<String> frankInBothGamesTotal = inBothGames.iterator().next();
    assertEquals("Frank", frankInBothGamesTotal.getValue());
    assertEquals(300.0, frankInBothGamesTotal.getScore());
    
    Set<TypedTuple<String>> inBothGamesWithMax = zSetOps.intersectWithScores("game1", Set.of("game2"), Aggregate.MAX);
    TypedTuple<String> frankInBothGamesMax = inBothGamesWithMax.iterator().next();
    assertEquals("Frank", frankInBothGamesMax.getValue());
    assertEquals(200.0, frankInBothGamesMax.getScore());
    
    Set<TypedTuple<String>> onlyInGame1 = zSetOps.differenceWithScores("game1", "game2");
    List<String> players = onlyInGame1.stream().map(t -> t.getValue()).collect(Collectors.toList());
    assertTrue(players.containsAll(Set.of("Pieter", "Ana")));
  }

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
