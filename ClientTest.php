<?php

require __DIR__.'/Client.php';

class SiderClientTest extends PHPUnit_Framework_TestCase {

    function testConnectProtoFailure() {
        $client = new sider_Client(array('protocol' => 'wurst'));
        try {
            $client->connect();
        } catch (Exception $e) {
            
        }
        if (!$e)
            $this->fail("expected exception not thrown");
        if ($e->getMessage() != "protocol(wurst) not supported") {
            throw $e;
        }
    }

    function testConnectHostFailure() {
        $client = new sider_Client(array('host' => 'veryverywrong'));
        try {
            $client->connect();
        } catch (Exception $e) {
            
        }
        if (!$e)
            $this->fail("expected exception not thrown");
        if ($e->getMessage() != "cannot resolve host(veryverywrong)") {
            throw $e;
        }
    }

    function testConnectPortFailure() {
        $this->markTestIncomplete('its tricky');
    }

    function testConnectAndDisconnect() {
        $client = new sider_Client();
        $connected = $client->connect();
        $this->assertTrue($connected);
        $disconnected = $client->disconnect();
        $this->assertTrue($disconnected);
    }

    function getClient() {
        static $client;
        if (!$client) {
            $client = new sider_Client();
            $client->connect();
        }
        $client->select(3);
        $client->flushdb();
        return $client;
    }
    
    function testPing() {
        $client = $this->getClient();
        $this->assertEquals("PONG", $client->ping());
    }

    function testNonsense() {
        $client = $this->getClient();
        try {
            $client->narwalbacon();
        } catch (Exception $e) {
            
        }
        if (!$e)
            $this->fail("expected exception not thrown");
        if ($e->getMessage() != "ERR unknown command 'narwalbacon'")
            throw $e;
    }
    
    function testSelect() {
        $client = $this->getClient();
        $ok = $client->select(3);
        $this->assertTrue($ok);
    }

    function testFlushDb() {
        $client = $this->getClient();
        $ok = $client->flushdb();
        $this->assertTrue($ok);   
    }

    function testFlushAll() {
        $client = $this->getClient();
        $ok = $client->flushall();
        $this->assertTrue($ok);   
    }

    function testDbSize() {
        $client = $this->getClient();
        $size = $client->dbsize();
        $this->assertEquals(0, $size);
    }
    
    function testInfo() {
        $client = $this->getClient();
        $this->assertTrue(strlen($client->info()) > 500);   
    }

    function testStrings() {
        $client = $this->getClient();
        $ok = $client->set("str", "val");
        $this->assertTrue($ok);
        $value = $client->get("str");
        $this->assertEquals("val", $value);
        $len = $client->append("str", "val");
        $this->assertEquals(6, $len);
        $value = $client->get("str");
        $this->assertEquals("valval", $value);
        $ok = $client->setnx("str", "newval");
        $this->assertEquals(0, $ok);
        $len = $client->strlen("str");
        $this->assertEquals(6, $len);
    }

    function testStringsMSetMGet() {
        $client = $this->getClient();
        $data = array(
            'str1' => 'v1',
            'str2' => 'v2',
        );
        $ok = $client->mset($data);
        $this->assertTrue($ok);
        $keys = array_keys($data);
        $keys[] = 'bacon';
        $storedData = $client->mget($keys);
        $expectedData = array_values($data);
        $expectedData[] = null;
        $this->assertEquals($expectedData, $storedData);
    }

    function testStringsCounter() {
        $client = $this->getClient();
        $count = $client->incr("narwals");
        $this->assertEquals(1, $count);   
        $count = $client->incrby("narwals", 10);
        $this->assertEquals(11, $count);   
        $count = $client->decr("narwals");
        $this->assertEquals(10, $count);   
        $count = $client->decrby("narwals", 20);
        $this->assertEquals(-10, $count);   
    }

    function testStringsBits() {
        $client = $this->getClient();
        $value = $client->setbit("narwals", 6, 1);
        $this->assertEquals(0, $value);   
        $value = $client->setbit("narwals", 6, 0);
        $this->assertEquals(1, $value);   
        $value = $client->getbit("narwals", 6);
        $this->assertEquals(0, $value);   
    }

    function testHash() {
        $client = $this->getClient();
        $ok = $client->hexists("narwals", "bacon");
        $this->assertEquals(0, $ok);
        $value = $client->hget("narwals", "bacon");
        $this->assertEquals(null, $value);   
        $value = $client->hset("narwals", "bacon", "yummy");
        $this->assertEquals(1, $value);   
        $ok = $client->hexists("narwals", "bacon");
        $this->assertEquals(1, $ok);
        $value = $client->hget("narwals", "bacon");
        $this->assertEquals("yummy", $value);   
        $hash = array(
            'bacon'   => 'vegan',
            'narwals' => 'mousse',
            'penguin' => 'butter',
        );
        $value = $client->hmset("narwals", $hash);
        $this->assertEquals(true, $value);   
        $keys = array_keys($hash);
        $keys[] = "zomboez";
        $value = $client->hmget("narwals", $keys);
        $values = array_values($hash);
        $values[] = null;
        $this->assertEquals($values, $value);
        $keys = $client->hkeys("narwals");
        $this->assertEquals(array_keys($hash), $keys);
        $keys = $client->hvals("narwals");
        $this->assertEquals(array_values($hash), $keys);
    }

    function testList() {
        $client = $this->getClient();
        $len = $client->rpushx("lst", "jelly");
        $this->assertEquals(0, $len)    ;
        $len = $client->rpush("lst", "jelly");
        $this->assertEquals(1, $len);
        $len = $client->rpush("lst", array("bacon", "cupcake"));
        $this->assertEquals(3, $len);
        $len = $client->lset("lst", 0, "nacho");
        $this->assertEquals(true, $len);
        $val = $client->rpop("lst");
        $this->assertEquals("cupcake", $val);
        $val = $client->lrange("lst", 0, 2);
        $this->assertEquals(array("nacho","bacon"), $val);
        $val = $client->llen("lst");
        $this->assertEquals(2, $val);
        $val = $client->ltrim("lst", 0, 0);
        $this->assertTrue($val);
        $val = $client->llen("lst");
        $this->assertEquals(1, $val);
        $val = $client->lrem("lst", 1, "nacho");
        $this->assertEquals(1, $val);
    }

    function testPipeline() {
        $client = $this->getClient();
        $client->pipe();
        $client->set("aa", "bb");
        $client->append("aa", "cc");
        $client->get("aa");
        $response = $client->unpipe();
        $expectedResponse = array(
            true, 4, 'bbcc'
        );
        $this->assertEquals($expectedResponse, $response);
    }

    function testPipelineIsReadMultipleTimes() {
        $client = new sider_Client(array(
            'readBuffer' => 0,
            'writeBuffer' => 0,
        ));
        $client->connect();
        $client->select(3);
        $client->flushdb();
        $client->pipe();
        $f = 1000000;
        $client->set("aa", str_repeat('X', $f));
        $i = 3;
        while ($i--) {
            $client->append("aa", str_repeat('Y', $f));
        }
        $response = $client->unpipe();
        $expectedResponse = array(
            true, 2*$f, 3*$f, 4*$f
        );
        $this->assertEquals($expectedResponse, $response);
        $expectedMetrics = array (
          'socketReads' => 4,
          'socketWrites' => 6,
          'commandsSend' => 6,
          'repliesReceived' => 6,
        );
        $this->assertEquals($expectedMetrics, $client->metrics());
    }

}
