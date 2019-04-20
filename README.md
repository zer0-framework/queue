# Queue
Компонент реализует очереди.

## Конфигурация
|Имя|     Тип|       Описание| Значение по-умолчанию|
|:-------:|:---:|:--------------:|:---------------------:|
|type|string| Тип хранилища |Redis

## Пример использования

```php
$pool = $this->app->factory('Queue');
try {
    var_dump($pool->enqueueWait(new SomeTask(), /* ждём ответа */ 3 /* секунды */)->foo);
    // string(3) "bar"
} catch (\Zer0\Queue\Exceptions\WaitTimeoutException $e) {
    // Задача не завершилась за 3 секунды
}
```

Так выглядит SomeTask:

```php
final class SomeTask extends \Zer0\Queue\TaskAbstract
{
    /**
     * @var string
     */
    public $foo;

    /**
     *
     */
    public function execute(): void
    {
        $this->foo = 'bar';

        $this->complete();
    }
}
```

