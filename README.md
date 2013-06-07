# Sphinx-search API provider
Provides one more abstract layer for query building

### Installation
comming soon in packagist...


#### Fast start
```php
    $pool = array (
                    array(
                        'host' => '192.168.200.22',
                        'port' => 9312
                    ),
                    array(
                        'host' => '192.168.200.23',
                        'port' => 9312
                    ),
                    array(
                        'host' => '192.168.200.24',
                        'port' => 9312
                    ),
            );
    $sph = new SphinxProvider($pool);
    $sph->addFilter('field', 1); // filter field = 1
    $sph->addFilter('field2', array(1,2,3), true); // filter field2 != [1, 2, 3]
    $sph->addFilter(array('field3', 'field4'), 5); // filters field3 = 5,field4 = 5
    $sph->setIndex('myindex');
    $sph->doApiRequest();
    //output format
    //  [data] => Array ( [0] => 7153679, [1] => 7153680 )
    //  [total] => 1
    //  [idsByOrder] => 7153679,7153680
    //  [total_found] => 2000
    //  [warnings] =>
    //  [errors] =>
    //  [time] => 0.054

```

#### Grouping (count) via provider
```php
    $sph->getGroupRequestFromApi('model_id');
    // [data] =>
    //   Array ( [0] =>
    //      Array ( [id] => 2 [cnt] => 421
    // ....
    // [total] => 1177
    // [total_found] => 1177
    // [time] => 0.066
```

#### grouping with additional fields returning
```php
    $sph->getGroupRequestFromApi('model_id', 'model_name asc', array('model_name', 'firm_name'));
    // [data] =>
        //   Array ( [0] =>
        //      Array ( [id] => 2 [cnt] => 421 [model_name] => 'Corolla' [firm_name] => 'Toyota'
        // ....
        // [total] => 1177
        // [total_found] => 1177
        // [time] => 0.066
