package org.example;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisTest {
    private Jedis jedis;
    @BeforeEach
    void setUp(){
//        jedis = new Jedis("101.201.215.139", 6379);
        jedis = JedisConnectionFactory.getJedis();
        jedis.auth("P@ssw0rd");
        jedis.select(0);
    }

    @Test
    void testString(){
        String result = jedis.set("name","虎哥");
        System.out.println(result);
        String name = jedis.get("name");
        System.out.println(name);
    }

    @AfterEach
    void tearDown(){
        if(jedis != null){
            jedis.close();
        }
    }
}
