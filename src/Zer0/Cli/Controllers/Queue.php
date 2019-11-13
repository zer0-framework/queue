<?php

namespace Zer0\Cli\Controllers;

use Zer0\Cli\AbstractController;
use Zer0\Cli\Controllers\Queue\Tap;
use Zer0\Cli\Controllers\Queue\Top;
use Zer0\Cli\Exceptions\InvalidArgument;
use Zer0\Queue\SomeTask;
use Zer0\Queue\TaskAbstract;

/**
 * Class Queue
 * @package Zer0\Cli\Controllers
 */
final class Queue extends AbstractController
{
    use Top;
    use Tap;

    /**
     * @var \Zer0\Queue\Pools\Base
     */
    protected $queue;

    /**
     * @var string
     */
    protected $command = 'queue';

    /**
     *
     */
    public function before(): void
    {
        parent::before();
        $this->queue = $this->app->factory('Queue');
    }

    /**
     * @param string $task
     * @throws InvalidArgument
     */
    public function enqueueAction(string $task = ''): void
    {
        $this->queue->enqueue($this->hydrateTask($task));
    }

    /**
     * @param string $task
     * @throws InvalidArgument
     */
    public function enqueueWaitAction(string $task = '', string $timeout = '10', string $extraFlags = ''): void
    {
        $task = $this->queue->enqueueWait($this->hydrateTask($task), (int)$timeout);
        $this->renderTask($task, $extraFlags);
    }

    /**
     * @param string $task
     */
    public function runInlineAction(string $task, string $extraFlags = ''): void
    {
        $task = $this->hydrateTask($task);
        $task->setCallback(function (TaskAbstract $task) use ($extraFlags) {
            $this->renderTask($task, $extraFlags);
        });
        $task();
    }

    /**
     * @param TaskAbstract $task
     * @param string $extraFlags
     */
    protected function renderTask(TaskAbstract $task, string $extraFlags = '')
    {
        $task->throwException();
        $this->cli->colorfulJson($task);
        $this->cli->writeln('');
        if (preg_match('~\bdebug\b~i', $extraFlags)) {
            $this->cli->writeln('');
            foreach ($task->getLog() as $item) {
                $this->cli->writeln("\t * " . $item);
            }
        }
        $this->cli->writeln('');
    }

    /**
     * @param string $str
     * @return TaskAbstract
     * @throws InvalidArgument
     */
    protected function hydrateTask(string $str): TaskAbstract
    {
        $split = explode(':', $str, 2);
        $class = $split[0];
        $properties = json_decode($split[1] ?? '{}');
        if (!class_exists($class)) {
            throw new InvalidArgument('Class ' . $class . ' not found.');
        }
        if (!is_a($class, TaskAbstract::class, true)) {
            throw new InvalidArgument('Class ' . $class . ' must be inherited from ' . TaskAbstract::class);
        }
        $task = new $class;
        foreach ($properties as $prop => $value) {
            $task->{$prop} = $value;
        }
        return $task;
    }

    /**
     *
     */
    public function testAction(): void
    {
        $this->cli->interactiveMode(true);
        for (; ;) {
            if (!$this->cli->interactiveMode()) {
                break;
            }
            $task = new SomeTask;
            $task->setChannel('test');
            $task->test = mt_rand(0, 10);
            $this->queue->enqueue($task);
            $this->cli->colorfulJson($task);
            $this->cli->writeln('');
            usleep(0.1 * 1e6);
        }
    }
}
