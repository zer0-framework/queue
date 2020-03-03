<?php

namespace Zer0\Queue;

use Zer0\Exceptions\BaseException;
use Zer0\Exceptions\InvalidArgumentException;
use Zer0\Exceptions\InvalidStateException;
use Zer0\Queue\Exceptions\RuntimeException;
use Zer0\Queue\Pools\BaseAsync;

/**
 * Class TaskAbstract
 *
 * @package Zer0\Queue
 */
abstract class TaskAbstract
{
    /**
     * @var callable
     */
    protected $callback;

    /**
     * @var string
     */
    private $_id;

    /**
     * @var string
     */
    private $_channel;

    /**
     * @var bool
     */
    protected $invoked = false;

    /**
     * @var bool
     */
    protected $finished = false;

    /**
     * @var RuntimeException
     */
    private $exception;

    /**
     * @var array
     */
    protected $log = [];

    /**
     * @var array
     */
    private $then = [];

    /**
     * @var BaseAsync
     */
    protected $queuePool;

    /**
     * @var float
     */
    protected $enqueuedAt;

    /**
     *
     */
    protected function before (): void
    {
    }

    /**
     * @return float|null
     */
    public function getEnqueuedAt (): ?float
    {
        return $this->enqueuedAt;
    }

    /**
     * Shall the task be put in queue again if it is not complete when timeout exceeds?
     *
     * @return bool
     */
    public function requeueOnTimeout (): bool
    {
        return true;
    }

    /**
     * @throws RuntimeException
     * @throws \Throwable
     */
    abstract protected function execute (): void;

    /**
     * @param BaseAsync|null $pool
     */
    final public function setQueuePool (?BaseAsync $pool): void
    {
        $this->queuePool = $pool;
    }

    /**
     * @param TaskAbstract $previous
     */
    public function previous (TaskAbstract $previous): void
    {
    }

    /**
     *
     */
    protected function after (): void
    {
        foreach ($this->then as $task) {
            $task->previous($this);
            $this->queuePool->enqueue($task);
        }
    }

    /**
     *
     */
    public function beforeEnqueue (): void
    {
        $this->enqueuedAt = microtime(true);
    }

    /**
     *
     */
    protected function onException (): void
    {
        // Subject to overload
    }

    /**
     * @return int
     */
    public function getTimeoutSeconds (): int
    {
        return 0;
    }

    /**
     * @return null|string ?string
     */
    final public function getId (): ?string
    {
        return $this->_id;
    }

    /**
     * @return null|string ?string
     */
    final public function getChannel (): ?string
    {
        return $this->_channel ?? 'default';
    }

    /**
     * @param $id
     */
    final public function setId (string $id): void
    {
        if ($id === '') {
            throw new InvalidArgumentException('$id cannot be empty');
        }
        if (!ctype_digit($id) && $this->getTimeoutSeconds() <= 0) {
            throw new InvalidArgumentException(
                'setId(): non-numeric id requires getTimeoutSeconds() to return an integer greater than 0'
            );
        }
        $this->_id = $id;
    }

    /**
     * @param string $channel
     */
    final public function setChannel (string $channel): void
    {
        $this->_channel = $channel;
    }

    /**
     * @param callable $callback
     *
     * @return self
     */
    final public function setCallback (callable $callback): TaskAbstract
    {
        $this->callback = $callback;

        return $this;
    }

    /**
     * @return bool
     */
    final public function invoked (): bool
    {
        return $this->invoked;
    }

    /**
     * @throws InvalidStateException
     */
    final public function __invoke ()
    {
        if ($this->invoked) {
            throw new InvalidStateException('The task instance has already been invoked.');
        }
        $this->invoked = true;
        try {
            $this->before();
            $this->execute();
        } catch (BaseException $exception) {
            $this->exception($exception);
        } catch (\Throwable $exception) {
            $this->exception(new RuntimeException('Uncaught exception ' . $exception));
        }
    }

    /**
     *
     */
    final protected function complete (): void
    {
        if ($this->finished) {
            return;
        }
        $this->finished = true;
        $callback       = $this->callback;
        $this->callback = null;
        $this->after();
        $this->then = [];
        $callback($this);
    }

    /**
     * @param BaseException $exception
     */
    final public function exception (BaseException $exception): void
    {
        if ($this->finished) {
            return;
        }
        $this->finished = true;
        $callback        = $this->callback;
        $this->callback  = null;
        $this->exception = $exception;
        $this->onException();
        $this->after();
        $callback($this);
    }

    /**
     * @param BaseException $exception
     */
    final public function setException (BaseException $exception): void
    {
        $this->exception = $exception;
    }

    /**
     * @param TaskAbstract $task
     *
     * @return $this
     */
    final public function then (TaskAbstract $task): self
    {
        $this->then[] = $task;

        return $this;
    }

    /**
     * @return null|\Throwable
     */
    final public function getException (): ?\Throwable
    {
        return $this->exception;
    }

    /**
     * @return bool
     */
    final public function hasException (): bool
    {
        return $this->exception !== null;
    }

    /**
     * @return TaskAbstract
     * @throws BaseException
     */
    final public function throwException (): self
    {
        if ($this->exception !== null) {
            throw $this->exception;
        }

        return $this;
    }

    /**
     * @return array
     */
    final public function getLog (): array
    {
        return $this->log;
    }

    /**
     * @param mixed ...$args
     */
    public function log (...$args): void
    {
        $this->log[] = sprintf(...$args);
    }

    /**
     * @return array
     */
    public function getObjectVars (): array
    {
        return array_diff_key(
            get_object_vars($this),
            [
                '_channel'  => true,
                'callback'  => true,
                '_id'       => true,
                'invoked'   => true,
                'exception' => true,
                'log'       => true,
            ]
        );
    }
}
