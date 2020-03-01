<?php

namespace Zer0\Queue;

use Zer0\Exceptions\BaseException;
use Zer0\Exceptions\InvalidStateException;
use Zer0\Queue\Exceptions\RuntimeException;
use Zer0\Queue\Exceptions\WaitTimeoutException;
use Zer0\Queue\Pools\Base;
use Zer0\Queue\Pools\BaseAsync;

/**
 * Class TaskCollection
 *
 * @package Zer0\Queue
 */
class TaskCollection
{
    /**
     * @var \SplObjectStorage
     */
    protected $pending;

    /**
     * @var \SplObjectStorage
     */
    protected $successful;

    /**
     * @var \SplObjectStorage
     */
    protected $failed;

    /**
     * @var \SplObjectStorage
     */
    protected $ready;

    /**
     * @var Base
     */
    protected $pool;

    /**
     * @var BaseAsync
     */
    protected $poolAsync;

    /**
     * @var callable
     */
    protected $callback;

    /**
     * TaskCollection constructor.
     *
     * @param TaskAbstract ...$tasks
     */
    public function __construct (...$tasks)
    {
        $this->pending    = new \SplObjectStorage;
        $this->successful = new \SplObjectStorage;
        $this->failed     = new \SplObjectStorage;
        $this->ready      = new \SplObjectStorage;

        foreach ($tasks as $task) {
            $this->add($task);
        }
    }

    /**
     * @param Base $pool
     */
    public function setPool (Base $pool): void
    {
        $this->pool = $pool;
    }

    /**
     * @param BaseAsync $pool
     */
    public function setPoolAsync (BaseAsync $pool): void
    {
        $this->poolAsync = $pool;
    }

    /**
     * @param TaskAbstract $task
     *
     * @return $this
     */
    public function add (TaskAbstract $task): self
    {
        if ($task->invoked()) {
            $this->ready->attach($task);
            if ($task->hasException()) {
                $this->failed->attach($task);
            }
            else {
                $this->successful->attach($task);
            }
        }
        else {
            if ($this->poolAsync !== null) {
                $this->poolAsync->enqueue($task);
            }
            else {
                $this->pool->enqueue($task);
            }
            $this->pending->attach($task);
        }

        return $this;
    }

    /**
     * @return bool
     */
    public function isEmpty (): bool
    {
        return $this->pending->count() === 0 && $this->ready->count() === 0;
    }

    /**
     * @param \Iterator $it
     * @param int       $maxPending = 1000
     *
     * @return $this
     */
    public function pull (\Iterator $it, int $maxPending = 1000): self
    {
        for (; ;) {
            if ($maxPending > 0) {
                if ($this->pending->count() >= $maxPending) {
                    return $this;
                }
            }
            if (!$it->valid()) {
                return $this;
            }
            $this->add($it->current());
            $it->next();
        }

        return $this;
    }

    /**
     * @return bool
     */
    public function hasPending (): bool
    {
        return $this->pending->count() > 0;
    }

    /**
     * @param callable $cb
     *
     * @return $this
     */
    public function callback (callable $cb): self
    {
        $this->callback = $cb;
        if ($this->poolAsync !== null) {
            $this->poolAsync->wait();
        }

        return $this;
    }

    /**
     *
     */
    public function free (): self
    {
        $this->callback = null;

        return $this;
    }

    /**
     * @param int $seconds
     *
     * @return $this
     */
    public function wait (int $seconds = 3): self
    {
        if ($this->poolAsync !== null) {
            $this->poolAsync->waitCollection($this, $this->callback, $seconds);

            return $this;
        }
        $this->pool->waitCollection($this, $seconds);
        if ($this->callback !== null) {
            ($this->callback)($this);
        }

        return $this;
    }

    /**
     * @param float $timeout
     *
     * @return $this
     */
    public function purgePending (float $timeout = 0): self
    {
        $time = microtime(true);
        foreach ($this->pending as $task) {
            /**
             * @var $task TaskAbstract
             */
            
            $max = max($timeout, $task->getTimeoutSeconds());
            if ($max <= 0) {
                continue;
            }
            $enqueuedAt = $task->getEnqueuedAt();
            if ($enqueuedAt === null) {
                continue;
            }
            if ($time > $enqueuedAt + $max) {
                $task->setException(new WaitTimeoutException('timeout exceeded'));
                $this->pending->detach($task);
                $this->ready->attach($task);
                $this->failed->attach($task);
            }
        }
        return $this;
    }

    /**
     * @param TaskAbstract $task
     */
    public function unlink (TaskAbstract $task): void
    {
        $this->successful->detach($task);
        $this->ready->detach($task);
        $this->failed->detach($task);
        $this->pending->detach($task);
    }

    /**
     * @return \SplObjectStorage
     */
    public function pending (): \SplObjectStorage
    {
        return $this->pending;
    }

    /**
     * @return \SplObjectStorage
     */
    public function successful (): \SplObjectStorage
    {
        return $this->successful;
    }

    /**
     * @return \SplObjectStorage
     */
    public function failed (): \SplObjectStorage
    {
        return $this->failed;
    }

    /**
     * @return \SplObjectStorage
     */
    public function ready (): \SplObjectStorage
    {
        return $this->ready;
    }
}
