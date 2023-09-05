<?php
namespace Crh\EsSearchLibrary;

use Crh\EsSearchLibrary\service\ElasticsearchService;

class EsSearch
{
    private $config;
    public function sayHello()
    {
        echo "Hello, World!";
    }

    public function __construct()
    {
        
        $this->config = require __DIR__.'/../config/es_config.php';
        foreach ($this->config as $k => $item){
            foreach($item as $kk => $vv){
                putenv("$k:$kk=$vv");
            }
        }

    }

    public function getEsClient(){
        static $elasticsearchService;

        if (!$elasticsearchService) {
            $elasticsearchService = new ElasticsearchService();
        }

        return $elasticsearchService;
    }

}