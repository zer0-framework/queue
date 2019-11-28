<?php

namespace Zer0\Queue;

use Zer0\Exceptions\BaseException;
use Zer0\Exceptions\InvalidStateException;
use Zer0\Queue\Exceptions\RuntimeException;
use Zer0\Queue\Pools\Base;

/**
 * Class TaskCollection
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
     * TaskCollection constructor.
     * @param TaskAbstract ...$args
     */
    public function __construct(Base $pool)
    {
        $this->pool = $pool;
        $this->pending = new \SplObjectStorage;
        $this->successful = new \SplObjectStorage;
        $this->failed = new \SplObjectStorage;
        $this->ready = new \SplObjectStorage;
    }

    /**
     * @param TaskAbstract $task
     * @return $this
     */
    public function add(TaskAbstract $task): self
    {
        $this->pool->enqueue($task);
        $this->pending->attach($task);
        return $this;
    }

    /**
     * @return bool
     */
    public function isEmpty(): bool
    {
        return $this->pending->count() === 0 && $this->ready->count() === 0;
    }

    /**
     * @param \Iterator $it
     * @param int $maxPending = 1000
     * @return $this
     */
    public function pull(\Iterator $it, int $maxPending = 1000): self
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
    public function hasPending(): bool
    {
        return $this->pending->count() > 0;
    }

    /**
     * @param int $seconds
     * @return $this
     */
    public function wait(int $seconds = 3): self
    {
        $this->pool->waitCollection($this, $seconds);
        return $this;
    }

    /**
     * @param TaskAbstract $task
     */
    public function unlink(TaskAbstract $task): void
    {
        $this->successful->detach($task);
        $this->ready->detach($task);
        $this->failed->detach($task);
        $this->pending->detach($task);
    }

    /**
     * @return \SplObjectStorage
     */
    public function pending(): \SplObjectStorage
    {
        return $this->pending;
    }

    /**
     * @return \SplObjectStorage
     */
    public function successful(): \SplObjectStorage
    {
        return $this->successful;
    }

    /**
     * @return \SplObjectStorage
     */
    public function failed(): \SplObjectStorage
    {
        return $this->failed;
    }

    /**
     * @return \SplObjectStorage
     */
    public function ready(): \SplObjectStorage
    {
        return $this->ready;
    }
}
