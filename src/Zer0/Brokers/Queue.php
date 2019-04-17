<?php

namespace Zer0\Brokers;

use PHPDaemon\Core\ClassFinder;
use Zer0\Config\Interfaces\ConfigInterface;

/**
 * Class Queue
 * @package Zer0\Brokers
 */
class Queue extends Base
{
    /**
     * @param ConfigInterface $config
     * @return \Zer0\Queue\Pools\Base
     */
    public function instantiate(ConfigInterface $config): \Zer0\Queue\Pools\Base
    {
        $class = ClassFinder::find($config->type, ClassFinder::getNamespace(\Zer0\Queue\Pools\Base::class), '~');
        return new $class($config, $this->app);
    }

    /**
     * @param string $name
     * @param bool $caching
     * @return \Zer0\Queue\Pools\Base
     */
    public function get(string $name = '', bool $caching = true): \Zer0\Queue\Pools\Base
    {
        return parent::get($name, $caching);
    }
}
