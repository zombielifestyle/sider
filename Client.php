<?php

class sider_Client {

    function __construct($options = array()) {
        $options = $this->options = array_merge(array(
            'protocol' => 'tcp',
            'host' => '127.0.0.1',
            'port' => 6379,
            'timeout' => 5,
            #'retries' => 5,
            'writeBuffer' => 8*1024,
            'readBuffer' => 8*1024,
        ), $options);
        $this->ip = null;
        $this->pipe = false;
        $this->pipedCommands = 0;
        $this->metrics = array(
            'socketReads' => 0,
            'socketWrites' => 0,
            'commandsSend' => 0,
            'repliesReceived' => 0,
        );
    }

    function connect() {
        if (false === getprotobyname($this->options['protocol'])) {
            throw new Exception("protocol(".$this->options['protocol'].") not supported");
        }
        if (is_null($this->ip)) {
            $this->ip = gethostbyname($this->options['host']);
        }
        if (false === ip2long($this->ip)) {
            throw new Exception("cannot resolve host(".$this->options['host'].")");
        }
        $errNo = $errStr = null;
        $addr = $this->options['protocol'].'://'.$this->ip.':'.$this->options['port'];
        $socket = stream_socket_client($addr, $errNo, $errStr, $this->options['timeout']);
        if (!$socket || $errNo) {
            throw new Exception("connection error($errNo:$errStr)");
        }
        stream_set_timeout($socket, $this->options['timeout']);
        stream_set_read_buffer($socket, $this->options['readBuffer']);
        stream_set_write_buffer($socket, $this->options['writeBuffer']);
        $this->socket = $socket;
        return true;
    }

    function disconnect() {
        if (is_resource($this->socket)) {
            return fclose($this->socket);
        }
        return false;
    }

    function pipe() {
        if ($this->pipe) {
            throw new Exception("pipeline already enabled");
        }
        $this->pipe = true;
        $this->pipedCommands = 0;
        return $this;
    }

    function unpipe() {
        $this->pipe = false;
        return $this->receive();
    }

    function metrics() {
        return $this->metrics;
    }

    function __call($method, $args) {
        $method = strtolower($method);
        $numArgs = count($args);
        if ($numArgs === 0) {
            $this->send("$method\r\n");
            $this->pipedCommands++;
            $this->metrics['commandsSend']++;
        } else {
            // argument 1 is a hash
            if ("mget" == $method) {
                $args = $args[0];
            // argument 2 is a list or value
            } else if ("hmget" == $method 
                    || "rpush" == $method
                    || "lpush" == $method
                    || "blpop" == $method
                    || "brpop" == $method) {
                $key = array_shift($args);
                if (is_array($args[0])) {
                    $args = $args[0];
                }
                array_unshift($args, $key);
            }/*
            } else if ("hmget" == $method 
                    || "rpush" == $method
                    || "lpush" == $method
                    || "blpop" == $method
                    || "brpop" == $method) {
                $key = array_shift($args);
                $args = $args[0];
                array_unshift($args, $key);
            }*/
            array_unshift($args, $method);
            $this->sendCommands(array($args));
        }
        if ($this->pipe) {
            return $this;
        }
        return $this->receive();
    }

    private function sendCommands($commands) {
        $s = '';
        $numArgs = 0;
        foreach ($commands as $command) {
            $this->pipedCommands++;
            $this->metrics['commandsSend']++;
            foreach ($command as $arg) {
                if (is_array($arg)) {
                    foreach ($arg as $k => $v) {
                        $s.= '$'.strlen($k)."\r\n".$k."\r\n";
                        $s.= '$'.strlen($v)."\r\n".$v."\r\n";
                        $numArgs += 2;
                    }
                } else {
                    $s.= '$'.strlen($arg)."\r\n".$arg."\r\n";
                    $numArgs += 1;
                }
            }
        }
        $this->send("*$numArgs\r\n$s");
    }

    private function send($msg) {
        $this->metrics['socketWrites']++;
        $len = strlen($msg);
        $written = fwrite($this->socket, $msg, $len);
        if (false === $written) {
            throw new Exception("write error");
        }
        if ($written < $len) {
            throw new Exception("write error, written($written) of len($len)");
        }
        return $written;
    }

    private function receive() {
        $replies = array();
        while ($this->pipedCommands > 0) {
            $response = '';
            do {
                $this->metrics['socketReads']++;
                $responsePart = fread($this->socket, 8192);
                $response.= $responsePart;
            } while (strlen($responsePart) == 8192);
            if (empty($response)) {
                throw new Exception("received empty response. protocol error or timeout");
            }
            $replies = array_merge($replies, $this->parse($response));
        }
        if (count($replies) === 1) {
            return reset($replies);
        }
        return $replies;
    }

    private function parse($response) {
        $replies = array();
        $multiBulkReplies = 0;
        $responseLen = strlen($response);
        for ($i = 0; $i < $responseLen;) {
            $type = $response{$i};
            $start = $i+1;

            if ($multiBulkReplies > 0) {
                $multiBulkReplies--;
            } else {
                $this->pipedCommands--;
                $this->metrics['repliesReceived']++;
            }

            while (true) {
                if ($response{$i} == "\r") {
                    $i+=2;
                    break;
                }
                ++$i;
            }

            $reply = substr($response, $start, $i - $start - 2);
            if ("-" == $type) {
                throw new Exception($reply);
            } else if ("+" == $type || ':' == $type) {
                if ($reply == 'OK') {
                    $reply = true;
                }
                $replies[] = $reply;
            } else if ("$" == $type) {
                $len = intval($reply);
                if ($len > 0) {
                    $replies[] = substr($response, $i, $len);
                    $i += $len + 2;
                } else {
                    $replies[] = null;
                    $i+=4;
                }
            } else if ("*" == $type) {
                $multiBulkReplies += intval($reply);
            } else {
                throw new Exception("parse error for reply type($type)");
            }
        }
        return $replies;
    }

}
