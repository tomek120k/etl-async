<?php declare(strict_types=1);

namespace Flow\ETL\Async\Worker;

interface Launcher
{
    public function launch(Pool $pool) : void;
}
