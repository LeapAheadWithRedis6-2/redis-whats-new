package com.redis.redis.sixtwo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.redis.connection.RedisListCommands.Direction.LEFT;
import static org.springframework.data.redis.connection.RedisListCommands.Direction.RIGHT;

import java.time.Duration;
import java.util.List;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ListOperations;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.testcontainers.RedisModulesContainer;

import io.lettuce.core.ClientOptions;

@Testcontainers
@SpringBootTest(classes = RedisListTest.Config.class)
class RedisListTest {

  @Container
  static final RedisModulesContainer REDIS = new RedisModulesContainer();

  @Resource(name = "stringRedisTemplate")
  private ListOperations<String, String> listOps;

  // - Add LMOVE and BLMOVE commands that pop and push arbitrarily (#6929)
  // - Add the COUNT argument to LPOP and RPOP (#8179)

  @Test
  void testSimpleExample() {
    // RPUSH funny_words "Shenanigans" "Bamboozle" "Bodacious"
    // LRANGE funny_words 0 -1
    // LPUSH funny_words "Bumfuzzle"
    // LRANGE funny_words 0 -1
    // LRANGE funny_words 1 3
    // LINSERT funny_words BEFORE "Bamboozle" "Brouhaha"
    // LRANGE funny_words 0 -1
    // LSET funny_words -2 "Flibbertigibbet"
    // LRANGE funny_words 0 -1
    // LPOP funny_words
    // LRANGE funny_words 0 -1
    listOps.rightPushAll("funny_words", "Shenanigans", "Bamboozle", "Bodacious");
    List<String> range = listOps.range("funny_words", 0, -1);
    assertArrayEquals(List.of("Shenanigans", "Bamboozle", "Bodacious").toArray(), range.toArray());

    listOps.leftPush("funny_words", "Bumfuzzle");

    range = listOps.range("funny_words", 1, 3);
    assertArrayEquals(List.of("Shenanigans", "Bamboozle", "Bodacious").toArray(), range.toArray());

    listOps.leftPush("funny_words", "Bamboozle", "Brouhaha");

    range = listOps.range("funny_words", 0, -1);
    assertArrayEquals(List.of("Bumfuzzle", "Shenanigans", "Brouhaha", "Bamboozle", "Bodacious").toArray(),
        range.toArray());
    
    listOps.set("funny_words", -2, "Flibbertigibbet");
    
    range = listOps.range("funny_words", 0, -1);
    assertArrayEquals(List.of("Bumfuzzle", "Shenanigans", "Brouhaha", "Flibbertigibbet", "Bodacious").toArray(),
        range.toArray());
    
    assertEquals(listOps.size("funny_words"), 5);
  }

  @Test
  void testLMOVE() {
    // RPUSH list_one "one" "two" "three"
    // LMOVE list_one list_two RIGHT LEFT
    // LMOVE list_one list_two LEFT RIGHT
    // LRANGE list_one 0 -1
    // 1) "two"
    // LRANGE list_two 0 -1
    // 1) "three"
    // 2) "one"
    listOps.rightPushAll("list_one", "one", "two", "three");
    listOps.move("list_one", RIGHT, "list_two", LEFT);
    listOps.move("list_one", LEFT, "list_two", RIGHT);

    List<String> listOne = listOps.range("list_one", 0, -1);
    List<String> listTwo = listOps.range("list_two", 0, -1);

    assertTrue(listOne.containsAll(List.of("two")));
    assertTrue(listTwo.containsAll(List.of("three", "one")));
  }

  @Test
  void testLPOP() {
    // redis> RPUSH mylist "one"
    // (integer) 1
    // redis> RPUSH mylist "two"
    // (integer) 2
    // redis> RPUSH mylist "three"
    // (integer) 3
    // redis> LPOP mylist 2
    // "one"
    // redis> LRANGE mylist 0 -1
    // 1) "three"
    listOps.rightPush("mylist", "one");
    listOps.rightPush("mylist", "two");
    listOps.rightPush("mylist", "three");
    listOps.leftPop("mylist", 2);
    List<String> myList = listOps.range("mylist", 0, -1);
    assertTrue(myList.containsAll(List.of("three")));
  }

  @Test
  void testRPOP() {
    // redis> RPUSH mylist "one"
    // (integer) 1
    // redis> RPUSH mylist "two"
    // (integer) 2
    // redis> RPUSH mylist "three"
    // (integer) 3
    // redis> RPOP mylist 2
    // "three"
    // redis> LRANGE mylist 0 -1
    // 1) "one"
    listOps.rightPush("mylist", "one");
    listOps.rightPush("mylist", "two");
    listOps.rightPush("mylist", "three");
    listOps.rightPop("mylist", 2);
    List<String> myList = listOps.range("mylist", 0, -1);
    assertTrue(myList.containsAll(List.of("one")));
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
