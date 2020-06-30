<?php

namespace Zer0\Queue\Pools;

use Zer0\App;
use Zer0\Config\Interfaces\ConfigInterface;
use Zer0\Queue\Exceptions\WaitTimeoutException;
use Zer0\Queue\TaskAbstract;
use Zer0\Queue\TaskCollection;

/**
 * Class Base
 *
 * @package Zer0\Queue\Pools
 */
abstract class Base
{
    /**
     * @var ConfigInterface
     */
    protected $config;

    /**
     * @var App
     */
    protected $app;

    /**
     * Base constructor.
     *
     * @param ConfigInterface $config
     * @param App             $app
     */
    public function __construct (ConfigInterface $config, App $app)
    {
        $this->config = $config;
        $this->app    = $app;
    }

    /**
     * Assign a sequential ID to $task if it's not assigned
     *
     * @param TaskAbstract $task
     */
    public function assignId (TaskAbstract $task): void
    {
        if ($task->getId() === null) {
            $task->setId($this->nextId());
        }
    }

    /**
     * Push $task into the queue
     *
     * @param TaskAbstract $task
     *
     * @return TaskAbstract
     */
    abstract public function push (TaskAbstract $task): TaskAbstract;

    /**
     * Push $task into the queue
     *
     * @param TaskAbstract $task
     * @deprecated
     * @return TaskAbstract
     */
    public function enqueue (TaskAbstract $task): TaskAbstract
    {
        return $this->push($task);
    }

    /**
     * Push $task into the queue and wait either until $task is ready or until $seconds timeout expires
     *
     * @param TaskAbstract $task
     * @param int          $seconds
     *
     * @return TaskAbstract
     * @throws WaitTimeoutException
     */
    final public function pushWait (TaskAbstract $task, int $seconds = 3): TaskAbstract
    {
        $this->enqueue($task);

        return $this->wait($task, $seconds);
    }

    /**
     * Push $task into the queue and wait either until $task is ready or until $seconds timeout expires
     *
     * @param TaskAbstract $task
     * @param int          $seconds
     *
     * @deprecated
     * @return TaskAbstract
     * @throws WaitTimeoutException
     */
    final public function enqueueWait (TaskAbstract $task, int $seconds = 3): TaskAbstract
    {
        return $this->pushWait($task, $seconds);
    }


    /**
     * Get a sequential task ID
     *
     * @return int
     */
    abstract public function nextId (): int;

    /**
     * Wait either until $task is ready or until $seconds timeout expires
     *
     * @param TaskAbstract $task
     * @param int          $seconds
     *
     * @return TaskAbstract
     * @throws WaitTimeoutException
     */
    abstract public function wait (TaskAbstract $task, int $seconds = 3): TaskAbstract;

    /**
     * Wait either until all tasks in $collection are ready or until $timeout expires
     *
     * @param TaskCollection $collection
     * @param float          $timeout
     *
     * @return void
     */
    abstract public function waitCollection (TaskCollection $collection, float $timeout = 1): void;

    /**
     * Make a collection
     *
     * @param TaskAbstract ...$tasks
     *
     * @return TaskCollection
     */
    final public function collection (...$tasks): TaskCollection
    {
        $collection = new TaskCollection(...$tasks);
        $collection->setPool($this);

        return $collection;
    }

    /**
     * Called when $task is complete
     *
     * @param TaskAbstract $task
     */
    abstract public function complete (TaskAbstract $task): void;

    /**
     * Pop the queue
     *
     * @param array|null $channels
     *
     * @return null|TaskAbstract
     */
    abstract public function pop (?array $channels = null): ?TaskAbstract;

    /**
     * Get a list of channels
     *
     * @return array
     */
    abstract public function listChannels (): array;

    /**
     * Take care of timed-out tasks
     *
     * @param string $channel
     *
     * @return
     */
    abstract public function timedOutTasks (string $channel);
}
