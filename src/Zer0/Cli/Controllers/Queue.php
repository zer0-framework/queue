<?php

namespace Zer0\Cli\Controllers;

use Zer0\Cli\AbstractController;
use Zer0\Cli\Controllers\Queue\Tap;
use Zer0\Cli\Controllers\Queue\Top;
use Zer0\Cli\Exceptions\InvalidArgument;
use Zer0\Queue\Exceptions\WaitTimeoutException;
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
    public function pushAction(string $task = ''): void
    {
        $this->queue->push($this->hydrateTask($task));
    }

    /**
     * 
     */
    public function channelsAction(): void
    {
        $this->cli->writeln(implode(', ', $this->queue->listChannels()));
    }

    public function infoAction(string $channel = 'default', string $measure = '') {

        $stats = $this->queue->getChannelStats($channel);

        if ($measure !== '') {
            $this->cli->writeln($stats[$measure] ?? '');
        } else {
            $this->cli->writeln('Backlog: ' . number_format($stats['backlog']));
            $this->cli->writeln('Total: ' . number_format($stats['total']));
            $this->cli->writeln( 'Complete: ' . number_format($stats['complete']));
        }
    }

    /**
     * @param string $task
     * @throws InvalidArgument
     */
    public function enqueueAction(string $task = ''): void
    {
        $this->pushAction(...func_get_args());
    }

    /**
     * @param string $task
     * @throws InvalidArgument
     */
    public function enqueueWaitAction(string $task = '', string $timeout = '10', string $extraFlags = ''): void
    {
        $this->pushWaitAction(...func_get_args());
    }

    /**
     * @param string $task
     * @throws InvalidArgument
     */
    public function pushWaitAction(string $task = '', string $timeout = '10', string $extraFlags = ''): void
    {
        try {
            $task = $this->queue->pushWait($this->hydrateTask($task), (int)$timeout);
            $this->renderTask($task, $extraFlags);
        } catch (WaitTimeoutException $e) {
            $this->cli->errorLine('Timeout exceeded.');
        }
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
        $debug = preg_match('~\bdebug\b~i', $extraFlags);
        if ($task->hasException()) {
            $output = (string) $task->getException();
            if (!$debug) {
                $output = strstr($output, "\nStack trace:", true);
            }
            $this->cli->errorLine($output);
        }
        $this->cli->colorfulJson($task->getObjectVars());
        $this->cli->writeln('');
        if ($debug) {
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
        $json = $split[1] ?? '{}';
        if (!class_exists($class)) {
            throw new InvalidArgument('Class ' . $class . ' not found.');
        }
        if (!is_a($class, TaskAbstract::class, true)) {
            throw new InvalidArgument('Class ' . $class . ' must be inherited from ' . TaskAbstract::class);
        }


        if (substr($json, 0, 1) === '{') {
            $properties = json_decode($json);
            $str = serialize($properties);
            $split = explode(':', $str, 4);
            $split[1] = strlen($class);
            $split[2] = rtrim(explode(':', serialize($class), 3)[2], ';');
            $str = join(':', $split);
            $task = unserialize($str);
        } elseif (substr($json, 0, 1) === '[') {
            $task = new $class(...json_decode($json));
        } else {
            throw new InvalidArgument('Invalid JSON: must be array/object');
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
            $this->queue->push($task);
            $this->cli->colorfulJson($task->getObjectVars());
            $this->cli->writeln('');
            usleep(0.1 * 1e6);
        }
    }
}
