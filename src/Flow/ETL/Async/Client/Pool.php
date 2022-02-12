<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Client;

use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;

final class Pool
{
    /**
     * @var array<UuidInterface>
     */
    private array $ids;

    /**
     * Pool constructor.
     *
     * @param array<UuidInterface> $ids
     */
    private function __construct(array $ids)
    {
        $this->ids = [];

        foreach ($ids as $id) {
            $this->ids[$id->toString()] = $id;
        }
    }

    public static function generate(int $size) : self
    {
        /** @psalm-suppress UnusedClosureParam */
        return new self(
            \array_map(fn (int $i) => Uuid::uuid4(), \range(0, $size))
        );
    }

    public function has(string $id) : bool
    {
        return \array_key_exists($id, $this->ids);
    }

    /**
     * @return UuidInterface[]
     */
    public function ids() : array
    {
        return $this->ids;
    }
}
