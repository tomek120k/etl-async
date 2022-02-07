<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Worker\CLI;

final class Input
{
    private array $argv;

    public function __construct(array $argv)
    {
        $this->argv = $argv;
    }

    public function optionValue(string $name, $default = null) : ?string
    {
        foreach ($this->argv as $arg) {
            $parts = \explode("=", $arg);

            if (\count($parts) !== 2) {
                continue;
            }

            if ($parts[0] === '--' . \strtolower($name)) {
                return $parts[1];
            }
        }

        return $default;
    }

    public function argv() : array
    {
        return $this->argv;
    }
}