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
    protected $done;

    /**
     * @var \SplObjectStorage
     */
    protected $failed;

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
        $this->done = new \SplObjectStorage;
        $this->failed = new \SplObjectStorage;
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
            if (!$task = $it->current()) {
                return $this;
            }
            $it->next();
            $this->add($task);
        }
        return $this;
    }

    /**
     * @return bool
     */
    public function isEmpty(): bool
    {
        return $this->pending->count() === 0;
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
     * @return \SplObjectStorage
     */
    public function pending(): \SplObjectStorage
    {
        return $this->pending;
    }

    /**
     * @return \SplObjectStorage
     */
    public function done(): \SplObjectStorage
    {
        return $this->done;
    }

    /**
     * @return \SplObjectStorage
     */
    public function failed(): \SplObjectStorage
    {
        return $this->failed;
    }
}
