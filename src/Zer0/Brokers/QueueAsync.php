<?php

namespace Zer0\Brokers;

use PHPDaemon\Core\ClassFinder;
use Zer0\Config\Interfaces\ConfigInterface;

/**
 * Class QueueAsync
 * @package Zer0\Brokers
 */
class QueueAsync extends Base
{
    /**
     * @var string
     */
    protected $broker = 'Queue';

    /**
     * @param ConfigInterface $config
     * @return \Zer0\Queue\Pools\BaseAsync
     */
    public function instantiate(ConfigInterface $config): \Zer0\Queue\Pools\BaseAsync
    {
        if ($config->type === 'ExtRedis') {
            $find = 'RedisAsync';
        } else {
            $find = $config->type . 'Async';
        }
        $class = ClassFinder::find($find, ClassFinder::getNamespace(\Zer0\Queue\Pools\BaseAsync::class), '~');
        return new $class($config, $this->app);
    }

    /**
     * @param string $name
     * @param bool $caching
     * @return \Zer0\Queue\Pools\BaseAsync
     */
    public function get(string $name = '', bool $caching = true): \Zer0\Queue\Pools\BaseAsync
    {
        return parent::get($name, $caching);
    }
}
