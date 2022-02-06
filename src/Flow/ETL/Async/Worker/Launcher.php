<?php

namespace Flow\ETL\Async\Worker;

interface Launcher
{
    public function launch(Pool $pool) : void;
}