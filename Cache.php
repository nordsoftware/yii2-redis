<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\redis;

use Yii;
use yii\base\Exception;
use yii\base\InvalidConfigException;

/**
 * Redis Cache implements a cache application component based on [redis](http://redis.io/) key-value store.
 *
 * Redis Cache requires redis version 2.6.12 or higher to work properly.
 *
 * It needs to be configured with a redis [[Connection]] that is also configured as an application component.
 * By default it will use the `redis` application component.
 *
 * See [[Cache]] manual for common cache operations that redis Cache supports.
 *
 * Unlike the [[Cache]], redis Cache allows the expire parameter of [[set]], [[add]], [[mset]] and [[madd]] to
 * be a floating point number, so you may specify the time in milliseconds (e.g. 0.1 will be 100 milliseconds).
 *
 * To use redis Cache as the cache application component, configure the application as follows,
 *
 * ~~~
 * [
 *     'components' => [
 *         'cache' => [
 *             'class' => 'yii\redis\Cache',
 *             'redis' => [
 *                 'hostname' => 'localhost',
 *                 'port' => 6379,
 *                 'database' => 0,
 *             ]
 *         ],
 *     ],
 * ]
 * ~~~
 *
 * Or if you have configured the redis [[Connection]] as an application component, the following is sufficient:
 *
 * ~~~
 * [
 *     'components' => [
 *         'cache' => [
 *             'class' => 'yii\redis\Cache',
 *             // 'redis' => 'redis' // id of the connection application component
 *         ],
 *     ],
 * ]
 * ~~~
 *
 * @author Carsten Brandt <mail@cebe.cc>
 * @since 2.0
 */
class Cache extends \yii\caching\Cache
{
    /**
     * @var Connection|string|array the Redis [[Connection]] object or the application component ID of the Redis [[Connection]].
     * This can also be an array that is used to create a redis [[Connection]] instance in case you do not want do configure
     * redis connection as an application component.
     * After the Cache object is created, if you want to change this property, you should only assign it
     * with a Redis [[Connection]] object.
     */
    public $redis = 'redis';


    /**
     * Initializes the redis Cache component.
     * This method will initialize the [[redis]] property to make sure it refers to a valid redis connection.
     * @throws InvalidConfigException if [[redis]] is invalid.
     */
    public function init()
    {
        parent::init();
        if (is_string($this->redis)) {
            $this->redis = Yii::$app->get($this->redis);
        } elseif (is_array($this->redis)) {
            if (!isset($this->redis['class'])) {
                $this->redis['class'] = Connection::className();
            }
            $this->redis = Yii::createObject($this->redis);
        }
        if (!$this->redis instanceof Connection) {
            throw new InvalidConfigException("Cache::redis must be either a Redis connection instance or the application component ID of a Redis connection.");
        }
    }

    /**
     * Checks whether a specified key exists in the cache.
     * This can be faster than getting the value from the cache if the data is big.
     * Note that this method does not check whether the dependency associated
     * with the cached data, if there is any, has changed. So a call to [[get]]
     * may return false while exists returns true.
     * @param mixed $key a key identifying the cached value. This can be a simple string or
     * a complex data structure consisting of factors representing the key.
     * @return boolean true if a value exists in cache, false if the value is not in the cache or expired.
     */
    public function exists($key)
    {
        return (bool)$this->redis->executeCommand('EXISTS', [$this->buildKey($key)]);
    }

    /**
     * @inheritdoc
     */
    protected function getValue($key)
    {
        return $this->redis->executeCommand('GET', [$key]);
    }

    /**
     *
     */
    protected function getHValues($key)
    {
        return $this->redis->executeCommand('HGETALL', [$key]);
    }

    /**
     *
     */
    protected function getHValue($key, $field)
    {
        return $this->redis->executeCommand('HGET', [$key, $field]);
    }

    /**
     * @inheritdoc
     */
    protected function getValues($keys, $command = 'MGET')
    {
        $response = $this->redis->executeCommand($command, $keys);
        $result = [];
        $i = 0;
        foreach ($keys as $key) {
            $result[$key] = $response[$i++];
        }

        return $result;
    }

    /**
     * Stores multiple items in cache. Each item contains a value identified by a key.
     * If the cache already contains such a key, the existing value and
     * expiration time will be replaced with the new ones, respectively.
     *
     * @param array $items the items to be cached, as key-value pairs.
     * @param integer $duration default number of seconds in which the cached values will expire. 0 means never expire.
     * @param Dependency $dependency dependency of the cached items. If the dependency changes,
     * the corresponding values in the cache will be invalidated when it is fetched via [[get()]].
     * This parameter is ignored if [[serializer]] is false.
     * @return boolean whether the items are successfully stored into cache
     * @TODO REWRITE THIS
     */
    public function hset($hashKey, $fieldName, $items, $duration = 0, $dependency = null)
    {
        if ($dependency !== null && $this->serializer !== false) {
            $dependency->evaluateDependency($this);
        }

        $data = [];
        if ($this->serializer === null) {
            $value = serialize([$items, $dependency]);
        } elseif ($this->serializer !== false) {
            $value = call_user_func($this->serializer[0], [$items, $dependency]);
        }

        $key = $this->buildKey($hashKey);

        return $this->setHValues($key, $fieldName, $value, $duration);
    }

    public function hget($key, $field)
    {
        $key = $this->buildKey($key);
        $value = $this->getHValue($key, $field);
        if ($value === false || $this->serializer === false) {
            return $value;
        } elseif ($this->serializer === null) {
            $value = unserialize($value);
        } else {
            $value = call_user_func($this->serializer[1], $value);
        }
        if (is_array($value) && !($value[1] instanceof Dependency && $value[1]->getHasChanged($this))) {
            return $value[0];
        } else {
            return false;
        }
    }

    /**
     * @TODO NOT working
     * @param $key
     * @return array|bool|mixed|null|string
     */
    public function hgetall($key)
    {
        $keyMap = [];
//        foreach ($keys as $key) {
//            $keyMap[$key] = $this->buildKey($key);
//        }
//        $values = $this->getValues(array_values($keyMap));
//        $results = [];
//        foreach ($keyMap as $key => $newKey) {
//            $results[$key] = false;
//            if (isset($values[$newKey])) {
//                if ($this->serializer === false) {
//                    $results[$key] = $values[$newKey];
//                } else {
//                    $value = $this->serializer === null ? unserialize($values[$newKey])
//                        : call_user_func($this->serializer[1], $values[$newKey]);
//
//                    if (is_array($value) && !($value[1] instanceof Dependency && $value[1]->getHasChanged($this))) {
//                        $results[$key] = $value[0];
//                    }
//                }
//            }
//        }
//
//
        $key = $this->buildKey($key);
        $value = $this->getHValues($key);
        if ($value === false || $this->serializer === false) {
            return $value;
        } elseif ($this->serializer === null) {
            foreach ($value as $_val) {
                
            }

            $value = unserialize($value);
        } else {
            $value = call_user_func($this->serializer[1], $value);
        }
        if (is_array($value) && !($value[1] instanceof Dependency && $value[1]->getHasChanged($this))) {
            return $value[0];
        } else {
            return false;
        }
    }

    protected function setHValues($hashKey, $fieldName, $value, $expire)
    {
        if ($expire == 0) {
            $return = $this->redis->executeCommand('HSET', [$hashKey, $fieldName, $value]);
            return (bool)$return;
        } else {
            $expire = (int)($expire * 1000);

            return (bool)$this->redis->executeCommand('HSET', [$hashKey, $fieldName, $value, 'PX', $expire]);
        }
    }

    /**
     * @inheritdoc
     */
    protected function setValue($key, $value, $expire)
    {
        if ($expire == 0) {
            return (bool)$this->redis->executeCommand('SET', [$key, $value]);
        } else {
            $expire = (int)($expire * 1000);
            return (bool)$this->redis->executeCommand('SET', [$key, $value, 'PX', $expire]);
        }
    }

    /**
     * @inheritdoc
     */
    protected function setValues($data, $expire)
    {
        $args = [];
        foreach ($data as $key => $value) {
            $args[] = $key;
            $args[] = $value;
        }

        $failedKeys = [];
        if ($expire == 0) {
            $this->redis->executeCommand('MSET', $args);
        } else {
            $expire = (int)($expire * 1000);
            $this->redis->executeCommand('MULTI');
            $this->redis->executeCommand('MSET', $args);
            $index = [];
            foreach ($data as $key => $value) {
                $this->redis->executeCommand('PEXPIRE', [$key, $expire]);
                $index[] = $key;
            }
            $result = $this->redis->executeCommand('EXEC');
            array_shift($result);
            foreach ($result as $i => $r) {
                if ($r != 1) {
                    $failedKeys[] = $index[$i];
                }
            }
        }

        return $failedKeys;
    }

    /**
     * @inheritdoc
     */
    protected function addValue($key, $value, $expire)
    {
        if ($expire == 0) {
            return (bool)$this->redis->executeCommand('SET', [$key, $value, 'NX']);
        } else {
            $expire = (int)($expire * 1000);

            return (bool)$this->redis->executeCommand('SET', [$key, $value, 'PX', $expire, 'NX']);
        }
    }

    /**
     * @inheritdoc
     */
    protected function deleteValue($key)
    {
        return (bool)$this->redis->executeCommand('DEL', [$key]);
    }

    /**
     * @inheritdoc
     */
    protected function flushValues()
    {
        return $this->redis->executeCommand('FLUSHDB');
    }
}
