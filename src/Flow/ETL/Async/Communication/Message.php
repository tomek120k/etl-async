<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Communication;

use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;
use Flow\Serializer\Serializable;

final class Message implements Serializable
{
    private string $type;

    /**
     * @var array<array<mixed>>
     */
    private array $payload;

    private function __construct(string $type, array $payload)
    {
        $this->type = $type;
        $this->payload = $payload;
    }

    public static function clientIdentify(string $id) : self
    {
        return new self(
            Protocol::CLIENT_IDENTIFY,
            ['id' => $id]
        );
    }

    public static function serverPipes(Pipes $pipes) : self
    {
        return new self(
            Protocol::SERVER_PIPES,
            ['pipes' => $pipes]
        );
    }

    public static function fetch() : self
    {
        return new self(
            Protocol::CLIENT_FETCH,
            []
        );
    }

    public static function rows(Rows $rows)
    {
        return new self(
            Protocol::SERVER_PROCESS,
            [
                'rows' => $rows
            ]
        );
    }

    public static function processed(Rows $rows)
    {
        return new self(
            Protocol::CLIENT_PROCESSED,
            [
                'rows' => $rows
            ]
        );
    }

    /**
     * @return array{id: String, payload: array<mixed>}
     */
    public function __serialize() : array
    {
        return [
            'type' => $this->type,
            'payload' => $this->payload
        ];
    }


    /**
     * @param array{id: String, payload: array<mixed>} $data
     * @return static
     */
    public function __unserialize(array $data) : void
    {
        $this->type = $data['type'];
        $this->payload = $data['payload'];
    }

    /**
     * @return string
     */
    public function type(): string
    {
        return $this->type;
    }

    /**
     * @return array<array<mixed>>
     */
    public function payload(): array
    {
        return $this->payload;
    }
}