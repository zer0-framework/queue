<?php

namespace Zer0\Queue\Pools;

use Google\Cloud\Tasks\V2\Task;
use PHPDaemon\Clients\Redis\Connection as RedisConnection;
use PHPDaemon\Clients\Redis\Pool;
use PHPDaemon\Core\Daemon;
use Zer0\App;
use Zer0\Config\Interfaces\ConfigInterface;
use Zer0\Queue\Exceptions\IncorrectStateException;
use Zer0\Queue\Exceptions\WaitTimeoutException;
use Zer0\Queue\TaskAbstract;
use Zer0\Queue\TaskCollection;

/**
 * Class RedisAsync
 *
 * @package Zer0\Queue\Pools
 */
final class RedisAsync extends BaseAsync
{

    /**
     * @var Pool
     */
    protected $redis;

    /**
     * @var string
     */
    protected $prefix;

    /**
     * @var int
     */
    protected $ttl;

    /**
     * Redis constructor.
     *
     * @param ConfigInterface $config
     * @param App             $app
     */
    public function __construct (ConfigInterface $config, App $app)
    {
        parent::__construct($config, $app);
        $this->redis  = $this->app->broker('RedisAsync')->get($config->name ?? '');
        $this->prefix = $config->prefix ?? 'queue';
        $this->ttl    = $config->ttl ?? 3600;
    }

    /**
     * @param iterable $tasks
     */
    public function updateTimeouts (iterable $tasks): void
    {
        $this->redis->multi(
            function (RedisConnection $redis) use ($tasks): void {
                foreach ($tasks as $task) {
                    /**
                     * @var $task TaskAbstract
                     */
                    $timeout = $task->getTimeoutSeconds();
                    if ($timeout > 0) {
                        $redis->zAdd(
                            $this->prefix . ':channel-pending:' . $task->getChannel(),
                            'XX',
                            time() + $timeout,
                            $task->getId()
                        );
                    }
                    $redis->exec();
                }
            }
        );
    }

    /**
     * @param TaskAbstract $task
     * @param string       $progress
     */
    public function setProgress (TaskAbstract $task, string $progress)
    {
        $timeout = $task->getTimeoutSeconds();
        if ($timeout > 0) {
            $this->redis->zAdd(
                $this->prefix . ':channel-pending:' . $task->getChannel(),
                'XX',
                time() + $timeout,
                $task->getId()
            );
        }
        $this->redis->publish($this->prefix . ':progress:' . $task->getId(), $progress);
    }

    /**
     * @param TaskAbstract $task
     * @param callable     $cb
     */
    public function subscribeProgress (TaskAbstract $task, callable $cb)
    {
        $this->redis->subscribe(
            $this->prefix . ':progress:' . $task->getId(),
            function (RedisConnection $redis) use ($cb): void {
                $cb($redis->result);
            }
        );

    }

    /**
     * @param TaskAbstract $task
     * @param callable     $cb (?TaskAbstract $task, BaseAsync $pool)
     */
    public function push (TaskAbstract $task, ?callable $cb = null): void
    {
        $taskId = $task->getId();
        if ($taskId === null) {
            $this->nextId(
                function (?string $id) use ($task, $cb) {
                    if ($id !== null) {
                        $task->setId($id);
                    }
                    $this->enqueue($task, $cb);
                }
            );

            return;
        }
        $autoId = ctype_digit($taskId);

        $task->beforeEnqueue();
        $task->beforePush();
        $channel = $task->getChannel();

        $payload = igbinary_serialize($task);
        $func    = function (RedisConnection $redis, bool $delayed = false) use ($cb, $task, $channel, $taskId, $autoId, $payload): void {
            $redis->publish($this->prefix . ':enqueue-channel:' . $channel, $payload);
            $redis->multi();
            $redis->sAdd($this->prefix . ':list-channels', $channel);
            if (!$delayed) {
                $redis->rPush($this->prefix . ':channel:' . $channel, $taskId);
            }
            $redis->incr($this->prefix . ':channel-total:' . $channel);
            $redis->setex($this->prefix . ':input:' . $taskId, $this->ttl, $payload);
            $redis->del($this->prefix . ':output:' . $taskId, $this->prefix . ':blpop:' . $taskId);
            $redis->exec(
                function (RedisConnection $redis) use ($cb, $task): void {
                    if ($cb !== null) {
                        $cb($task, $this);
                    }
                }
            );
        };

        if ($task->getDelay() > 0) {
            $this->redis->zadd(
                $this->prefix . ':channel-pending:' . $channel,
                $task->getDelayOverwrite() ? null : 'NX',
                time() + $task->getDelay(),
                $taskId,
                function (RedisConnection $redis) use ($func, $task, $cb): void {
                    if (!$redis->result) {
                        if ($cb !== null) {
                            $cb($task, $this);
                        }

                        return;
                    }
                    $func($redis, true);
                }
            );
        }
        else if (!$autoId && $task->getTimeoutSeconds() > 0) {
            $this->redis->zadd(
                $this->prefix . ':channel-pending:' . $channel,
                'NX',
                time() + $task->getTimeoutSeconds(),
                $taskId,
                function (RedisConnection $redis) use ($func, $task, $cb): void {
                    if (!$redis->result) {
                        if ($cb !== null) {
                            $cb($task, $this);
                        }

                        return;
                    }
                    $func($redis);
                }
            );
        }
        else {
            $this->redis->getConnection(
                null,
                function (RedisConnection $redis) use ($func): void {
                    $func($redis);
                }
            );
        }
    }

    /**
     * @param callable $cb (int $id)
     */
    public function nextId (callable $cb): void
    {
        $this->redis->incr(
            $this->prefix . ':task-seq',
            function (RedisConnection $redis) use ($cb) {
                $cb($redis->result);
            }
        );
    }

    /**
     * @param TaskAbstract $task
     * @param int          $timeout
     * @param callable     $cb (?TaskAbstract $task)
     *
     * @throws IncorrectStateException
     */
    public function wait (TaskAbstract $task, int $timeout, callable $cb): void
    {
        $taskId = $task->getId();
        if ($taskId === null) {
            throw new IncorrectStateException('\'id\' property must be set before wait() is called');
        }
        $this->redis->blPop(
            $this->prefix . ':blpop:' . $taskId,
            $timeout,
            function (RedisConnection $redis) use ($taskId, $cb, $task) {
                if (!$redis->result) {
                    $cb($task);

                    return;
                }
                $this->redis->get(
                    $this->prefix . ':output:' . $taskId,
                    function (RedisConnection $redis) use ($cb) {
                        if ($redis->result === null) {
                            $task->exception(new IncorrectStateException('empty output'));
                            $cb($task);
                        }
                        try {
                            $cb(igbinary_unserialize($redis->result));
                        } catch (\Throwable $e) {
                            $task->exception($e);
                            $cb($task);
                        }
                    }
                );
            }
        );
    }

    /**
     * @param TaskCollection $collection
     * @param callable       $cb
     * @param float          $timeout
     */
    public function waitCollection (TaskCollection $collection, callable $cb, float $timeout = 1): void
    {
        $hash = [];
        foreach ($collection->pending() as $task) {
            $key  = $this->prefix . ':blpop:' . $task->getId();
            $item =& $hash[$key];
            if ($item === null) {
                $item = [];
            }
            $item[] = $task;
        }

        if (!$hash) {
            return;
        }

        $cb($collection);

        $pop = $redis->result;
        if ($pop === null) {
            return;
        }
        $key = array_key_first($pop);

        $tasks = $hash[$key] ?? null;
        if ($tasks === null) {
            return;
        }
        unset($hash[$key]);

        $taskId = $tasks[0]->getId();

        $this->redis->get(
            $this->prefix . ':output:' . $taskId,
            function (RedisConnection $redis) use ($collection, $tasks, $cb) {
                $payload = $redis->result;

                $pending    = $collection->pending();
                $successful = $collection->successful();
                $ready      = $collection->ready();
                $failed     = $collection->failed();

                if ($payload !== null) {
                    $task = igbinary_unserialize($payload);
                }
                else {
                    /**
                     * @var $task TaskAbstract
                     */
                    $task = $tasks[0];
                    $task->setException(
                        new IncorrectStateException($this->prefix . ':output:' . $taskId . ' key does not exist')
                    );
                }
                $ready->attach($task);
                if ($task->hasException()) {
                    $failed->attach($task);
                }
                else {
                    $successful->attach($task);
                }
                $cb($collection);
            }
        );
    }

    /**
     * @param array|null $channels
     * @param callable   $cb (TaskAbstract $task)
     */
    public function pop (?array $channels, callable $cb): void
    {
        if ($channels === null) {
            $this->listChannels(
                function (?array $channels) use ($cb) {
                    if ($channels === null) {
                        $cb(null);

                        return;
                    }
                    $this->pop($channels, $cb);
                }
            );

            return;
        }
        if (!count($channels)) {
            setTimeout(
                function () use ($cb) {
                    $cb(null);
                },
                1e6
            );

            return;
        }

        $keys = [];
        foreach ($channels as $chan) {
            $keys[] = $this->prefix . ':channel:' . $chan;
        }

        $this->redis->blPop(
            ...array_merge(
                $keys,
                [
                    1,
                    function (?RedisConnection $redis) use ($cb) {
                        if (!$redis || !$redis->result) {
                            $cb(null);

                            return;
                        }
                        [$key, $taskId] = $redis->result;
                        $channel = substr($key, strlen($this->prefix . ':channel:'));

                        $this->redis->get(
                            $this->prefix . ':input:' . $taskId,
                            function (RedisConnection $redis) use ($channel, $cb) {
                                if (!$redis->result) {
                                    $cb(null);

                                    return;
                                }
                                try {
                                    $task = igbinary_unserialize($redis->result);
                                    $cb($task);
                                } catch (\Throwable $e) {
                                    $cb(null);
                                }
                            }
                        );
                    },
                ]
            )
        );
    }

    /**
     * @param callable $cb (?array $channels)
     */
    public function listChannels (callable $cb): void
    {
        $this->redis->sMembers(
            $this->prefix . ':list-channels',
            function (RedisConnection $redis) use ($cb) {
                $cb((array)$redis->result);
            }
        );

        return;
    }

    /**
     * @param string $channel
     */
    public function timedOutTasks (string $channel): void
    {
        $zset = $this->prefix . ':channel-pending:' . $channel;
        $this->redis->zRangeByScore(
            $zset,
            '-inf',
            time(),
            'LIMIT',
            0,
            1000,
            function (RedisConnection $redis) use ($zset, $channel) {
                if (!$redis->result) {
                    return;
                }
                if (!is_array($redis->result)) {
                    Daemon::$process->log(
                        (string)new \Exception(
                            'Expected array, given: ' . var_export($redis->result, true)
                        )
                    );

                    return;
                }
                $zrange = $redis->result;
                $args   = [];
                foreach ($zrange as $value) {

                    $taskId = $value;

                    $redis->zRem(
                        $zset,
                        $value,
                        function (RedisConnection $redis) use ($taskId) {
                            if (!$redis->result) {
                                return;
                            }
                            $redis->get(
                                $this->prefix . ':input:' . $taskId,
                                function (RedisConnection $redis) use ($taskId) {
                                    if (!$redis->result) {
                                        return;
                                    }
                                    try {
                                        /**
                                         * @var $task TaskAbstract
                                         */
                                        $task = igbinary_unserialize($redis->result);
                                        if ($task->getDelay() > 0 || $task->requeueOnTimeout()) {
                                            $task->setDelay(0);
                                            $this->push($task);
                                        }
                                    } catch (\Throwable $e) {
                                    }
                                }
                            );
                        }
                    );
                }
            }
        );
    }

    /**
     * @param TaskAbstract $task
     */
    public function complete (TaskAbstract $task): void
    {
        if (isset($task->_force_sync_complete)) { //  @TODO: REMOVE
            App::instance()->factory('Queue')->complete($task);
            return;
        }
        $payload = igbinary_serialize($task);
        $taskId  = $task->getId();
        Daemon::log('[' . $taskId . '] complete()');
        $redis = $this->redis;
        $redis->publish($this->prefix . ':output:' . $taskId, $payload);

        $channel = $task->getChannel();
        $redis->publish($this->prefix . ':complete-channel:' . $channel, $payload);
        $redis->zRem(
            $this->prefix . ':channel-pending:' . $channel,
            $task->getId()
        );
        $redis->setex($this->prefix . ':output:' . $taskId, $this->ttl, $payload);
        $redis->expire($this->prefix . ':output:' . $taskId, 15 * 60);

        $redis->rPush($this->prefix . ':blpop:' . $taskId, ...range(1, 10));
        $redis->expire($this->prefix . ':blpop:' . $taskId, 10);

        $redis->del($this->prefix . ':input:' . $taskId);

        Daemon::log('[' . $taskId . '] redis exec call');
    }
}
