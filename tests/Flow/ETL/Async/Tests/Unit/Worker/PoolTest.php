<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Tests\Unit\Worker;

use Flow\ETL\Async\Client\Pool;
use PHPUnit\Framework\TestCase;

final class PoolTest extends TestCase
{
    public function test_checking_if_id_exists_in_pool() : void
    {
        $pool = Pool::generate(5);
        $this->assertFalse($pool->has('not-existig-id'));
        $this->assertTrue($pool->has(\current($pool->ids())->toString()));
    }
}
