<?php
use Crh\EsSearchLibrary\service\EnvManager;

return [
    'elasticsearch' => [
        'host' => EnvManager::get('host', 'host.docker.internal'),
        'port' => EnvManager::get('port', '9200'),
        'scheme' => EnvManager::get('scheme', 'http'),
        'user' => EnvManager::get('user', ''),
        'pass' => EnvManager::get('pass', ''),
    ]
]

?>