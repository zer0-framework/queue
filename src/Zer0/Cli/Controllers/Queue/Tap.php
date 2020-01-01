<?php

namespace Zer0\Cli\Controllers\Queue;

use Zer0\Queue\TaskAbstract;

/**
 * Trait Tap
 * @package Zer0\Cli\Controllers\Queue
 */
trait Tap
{

    /**
     * @param string $channel
     */
    public function tapAction(string $channel = null, string $filter = null): void
    {
        $this->cli->interactiveMode(true);
        $this->cli->asyncSignals(true);
        $this->queue->subscribe(
            $channel ?? 'default',
            function (?string $event, ?TaskAbstract $task) use ($filter): bool {
                static $styleSheet = [
                    'new' => 'fg(blue) i',
                    'complete' => 'fg(green) i',
                    'error' => 'fg(red) i',
                ];

                if ($event !== null) {
                    if ($filter !== null && $event !== $filter) {
                        return true;
                    }
                    if ($task->hasException()) {
                        $event = 'error';
                    }

                    $this->cli->write(strtoupper($event), $styleSheet[$event]);
                    $this->cli->write(str_repeat(' ', 15 - strlen($event)));

                    $this->cli->write($task->getId());
                    $this->cli->write("\t");

                    $this->cli->write(get_class($task));
                    $this->cli->write(':');

                    $this->cli->colorfulJson($task->getObjectVars());

                    $this->cli->writeln('');

                    if ($task->hasException()) {
                        $this->cli->writeln("⇧\t" . $task->getException()->getMessage());
                    }
                }
                if (!$this->cli->interactiveMode()) {
                    return false;
                }
                return true;
            }
        );
        if (!$this->cli->interactiveMode()) {
            $this->cli->writeln('');
            return;
        }
    }
}
