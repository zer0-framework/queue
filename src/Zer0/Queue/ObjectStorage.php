<?php

namespace Zer0\Queue;

class ObjectStorage extends \SplObjectStorage
{
    /**
     * @return iterable
     */
    public function it(): iterable
    {
        $this->rewind();
        while ($this->valid()) {
            $obj = $this->current();
            $this->next();
            yield $obj;
        }
    }
}
