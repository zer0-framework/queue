<?php

namespace Zer0\Queue\Worker;

use PHPDaemon\Core\Daemon;
use PHPDaemon\Core\Timer;
use Zer0\App;
use Zer0\Queue\Pools\BaseAsync;
use Zer0\Queue\SomeTask;
use Zer0\Queue\TaskAbstract;

/**
 * Class Application
 *
 * @package InterpalsD
 */
final class Application extends \PHPDaemon\Core\AppInstance
{
    /**
     * @var App
     */
    public $app;

    /**
     * @var BaseAsync
     */
    protected $pool;

    /**
     * @var \SplObjectStorage
     */
    protected $tasks;

    /**
     * Called when the worker is ready to go.
     *
     * @return void
     */
    public function onReady ()
    {

        require_once ZERO_ROOT . '/vendor/zer0-framework/core/src/bootstrap.php';

        defined('ZERO_ASYNC') || define('ZERO_ASYNC', 1);

        $this->app = App::instance();

        $this->tasks = new \SplObjectStorage;

        $this->pool = $this->app->factory('QueueAsync', $this->config->name ?? '');
        $this->pop();

        setTimeout(
            function (Timer $timer): void {
                $this->pool->listChannels(
                    function (array $channels): void {
                        foreach ($channels as $channel) {
                            $this->pool->timedOutTasks($channel);
                        }
                    }
                );

                $this->pool->updateTimeouts($this->tasks);

                $timer->timeout(5e6);
            },
            1
        );
    }

    /**
     *
     */
    public function pop ()
    {
        if (isset($this->config->maxconcurrency->value)
            && $this->tasks->count() > $this->config->maxconcurrency->value) {
            setTimeout(
                function (Timer $timer): void {
                    $this->pop();
                    $timer->free();
                },
                1e6
            );

            return;
        }
        $channels = $this->config->channels->value ?? null;
        $this->pool->pop(
            $channels ? (array)$channels : null,
            function (?TaskAbstract $task) {
                try {
                    if (!$task) {
                        return;
                    }
                    $this->tasks->attach($task);
                    $task->setQueuePool($this->pool);
                    $task->setCallback(
                        function (TaskAbstract $task) {
                            $task->setQueuePool(null);
                            $this->pool->complete($task);
                            $this->tasks->detach($task);
                        }
                    );
                    Daemon::$process->setState(Daemon::WSTATE_BUSY);
                    $task();
                    Daemon::$process->setState(Daemon::WSTATE_IDLE);
                } finally {
                    if (!Daemon::$process->isTerminated() && !Daemon::$process->reload) {
                        $this->pop();
                    }
                }
            }
        );
    }
}
