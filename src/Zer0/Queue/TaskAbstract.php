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
     * @var int
     */
    protected $_delay = 0;

    /**
     * @var bool
     */
    protected $_delayOverwrite = false;

    /**
     *
     */
    protected function before(): void
    {
    }

    /**
     * @return float|null
     */
    public function getEnqueuedAt(): ?float
    {
        return $this->enqueuedAt;
    }

    /**
     * Shall the task be put in queue again if it is not complete when timeout exceeds?
     *
     * @return bool
     */
    public function requeueOnTimeout(): bool
    {
        return true;
    }

    /**
     * @return string
     */
    public function currentProgress(): string
    {
        return '';
    }

    /**
     * @throws RuntimeException
     * @throws \Throwable
     */
    abstract protected function execute(): void;

    /**
     * @param BaseAsync|null $pool
     */
    final public function setQueuePool(?BaseAsync $pool): void
    {
        $this->queuePool = $pool;
    }

    /**
     * @param string $progress
     */
    final public function setProgress(string $progress): void
    {
        if ($this->queuePool === null) {
            return;
        }
        $this->queuePool->setProgress($this, $progress);
    }

    /**
     * @param TaskAbstract $previous
     */
    public function previous(TaskAbstract $previous): void
    {
    }

    /**
     *
     */
    protected function after(): void
    {
        foreach ($this->then as $task) {
            $task->previous($this);
            $this->queuePool->push($task);
        }
    }

    /**
     * @return $this
     */
    public function resetState(): self
    {
        $this->invoked = false;
        $this->exception = null;
        $this->finished = false;
        if (ctype_digit($this->_id)) {
            $this->_id = null;
        }
        return $this;
    }

    /**
     * Called before pushing into the queue
     */
    public function beforePush(): void
    {
        $this->enqueuedAt = microtime(true);
    }

    /**
     * @deprecated
     */
    public function beforeEnqueue(): void
    {
    }

    /**
     * @param \Throwable $exception
     */
    protected function onException(\Throwable $exception): void
    {
        // Subject to overloading
    }

    /**
     * @return int
     */
    public function getTimeoutSeconds(): int
    {
        return 0;
    }

    /**
     * @return int
     */
    public function getDelay(): int
    {
        return $this->_delay;
    }

    /**
     * @return bool
     */
    public function getDelayOverwrite(): bool
    {
        return $this->_delayOverwrite;
    }

    /**
     * @param int $delay
     * @param bool $overwrite
     */
    final public function setDelay(int $delay, bool $overwrite = false): void
    {
        $this->_delay = $delay;
        $this->_delayOverwrite = $overwrite;
    }


    /**
     * @return null|string ?string
     */
    final public function getId(): ?string
    {
        return $this->_id;
    }

    /**
     * @return null|string ?string
     */
    final public function getChannel(): ?string
    {
        return $this->_channel ?? 'default';
    }

    /**
     * @param $id
     */
    final public function setId(string $id): void
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
    final public function setChannel(string $channel): void
    {
        $this->_channel = $channel;
    }

    /**
     * @param callable $callback
     *
     * @return self
     */
    final public function setCallback(callable $callback): TaskAbstract
    {
        $this->callback = $callback;

        return $this;
    }

    /**
     * @return bool
     */
    final public function invoked(): bool
    {
        return $this->invoked;
    }

    /**
     * @throws InvalidStateException
     */
    final public function __invoke()
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
    final protected function complete(): void
    {
        if ($this->finished) {
            return;
        }
        $this->finished = true;
        $callback = $this->callback;
        $this->callback = null;
        $this->after();
        $this->then = [];
        $callback($this);
    }

    /**
     * @param BaseException $exception
     */
    final public function exception(BaseException $exception): void
    {
        if ($this->finished) {
            return;
        }
        $this->finished = true;
        $callback = $this->callback;
        $this->callback = null;
        $this->exception = $exception;
        $this->onException($exception);
        $this->after();
        $callback($this);
    }

    /**
     * @param BaseException $exception
     */
    final public function setException(BaseException $exception): void
    {
        $this->exception = $exception;
    }

    /**
     * @param TaskAbstract $task
     *
     * @return $this
     */
    final public function then(TaskAbstract $task): self
    {
        $this->then[] = $task;

        return $this;
    }

    /**
     * @return null|\Throwable
     */
    final public function getException(): ?\Throwable
    {
        return $this->exception;
    }

    /**
     * @return bool
     */
    final public function hasException(): bool
    {
        return $this->exception !== null;
    }

    /**
     * @return TaskAbstract
     * @throws BaseException
     */
    final public function throwException(): self
    {
        if ($this->exception !== null) {
            throw $this->exception;
        }

        return $this;
    }

    /**
     * @return array
     */
    final public function getLog(): array
    {
        return $this->log;
    }

    /**
     * @param mixed ...$args
     */
    public function log(...$args): void
    {
        $this->log[] = sprintf(...$args);
    }

    /**
     * @return array
     */
    public function getObjectVars(): array
    {
        return array_diff_key(
            get_object_vars($this),
            [
                '_channel' => true,
                'callback' => true,
                '_id' => true,
                'invoked' => true,
                'finished' => true,
                'queuePool' => true,
                'enqueuedAt' => true,
                'exception' => true,
                'log' => true,
                'then' => true,
            ]
        );
    }
}
