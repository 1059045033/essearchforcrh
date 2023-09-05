<?php
namespace Crh\EsSearchLibrary\service;

use Elasticsearch\ClientBuilder;

class ElasticsearchService
{
    protected $client;

    private $_search_params = [];
    private $_search_where = [];
    private $combinedQuery = [
        'bool' => [
            'must' => [],
            'should' => [],
            'must_not' => [],
        ],
    ];

    private $_search_nums = 0; // 查询结果的条数
    private $_search_size = 10; // 默认查询条数

    private $_field_str = [];
    private $_field_as_name = [];

    # 聚合方法 映射
    private $mysql_mapping_to_es_aggs_function = [
        'sum'         => 'sum',
        'avg'         => 'avg',
        'min'         => 'min',
        'max'         => 'max',
        'count' => 'value_count',
    ];

    # 比较运算符 映射
    private $mysql_mapping_to_es_comparison = [
        '>'  => '>',
        '<'  => '<',
        '>=' => '>=',
        '<=' => '<=',
        '='  => '==',
        '<>' => '!=',
    ];

    # 逻辑运算符 映射
    private $mysql_mapping_to_es_logical = [
        'and' => '&&',
        'or'  => '||',
    ];

    public function __construct()
    {
        $this->client = ClientBuilder::create()->setHosts([
            [
                'host'   => getenv('elasticsearch:host'),//'localhost',//'host.docker.internal',//Env::get('elasticsearch.host', ''),
                'port'   => getenv('elasticsearch:port'),//9200,//Env::get('elasticsearch.port', ''),
                'scheme' => getenv('elasticsearch:scheme'),//'http',//Env::get('elasticsearch.scheme', ''),
                'user'   => getenv('elasticsearch:user'),//'',//Env::get('elasticsearch.user', ''),
                'pass'   => getenv('elasticsearch:pass')//'',//Env::get('elasticsearch.pass', ''),
            ],
        ])->build();
    }

    /**
     * @param $index 索引名称
     * @param $settings 配置 eg : $settings = ['number_of_shards' => 5,'number_of_replicas' => 1]; 参数:分片数量  副本数量
     * @param $mappings 映射文档属性 eg :$mappings = ['properties' => ['field1' => ['type' => 'text'], 'field2' => ['type' => 'keyword'],]];
     *
     * @return array
     */
    public function createIndex($index, $settings = [], $mappings = [])
    {
        $params = [
            'index' => $index,
            'body' => [
                'settings' => $settings,
                'mappings' => $mappings,
            ],
        ];

        return $this->client->indices()->create($params);
    }

    /**
     * 查询索引文档
     * @return array|callable
     */
    public function _table($index)
    {
        $this->_search_params['index'] = $index;
        $this->_search_params['body'] = [];
        return $this;
    }

    public function _size($size = 10)
    {

        $this->_search_params['body']['size'] = $this->_search_size = $size;
        return $this;
    }

    public function _page_num($page = 1)
    {
        $this->_search_params['body']['from'] = ($page - 1) * $this->_search_params['body']['size'];
        return $this;
    }

    /**
     * @param $where_arr
     * @param $type  匹配查询match | 多字段匹配查询multi_match(暂时不实现) | 短语匹配查询match_phrase | 范围查询range | 精确匹配查询term
     *
     * @return void
     */
    public function _where($where_arr, $type = 'term')
    {
        if ($type == 'match' || $type == 'term' || $type == 'range') {
            foreach ($where_arr as $k => $v) {
                $this->combinedQuery['bool']['must'][] = [
                    $type => [$k => $v]
                ];
            }
        }

        $this->_search_params['body']['query'] = $this->combinedQuery;
        return $this;
    }

    /**
     * @param $where_arr
     *
     * @return $this
     */
    public function _where_range($where_arr)
    {
        foreach ($where_arr as $k => $v) {
            foreach ($where_arr as $k => $v) {
                $this->combinedQuery['bool']['must'][] = [
                    'range' => [$k => $v]
                ];
            }
        }


        $this->_search_params['body']['query'] = $this->combinedQuery;
        //echo json_encode($this->combinedQuery);die;


        return $this;
    }

    /**
     * 满足一个条件即可
     */
    public function _where_or($where_arr, $type = 'match')
    {
        if ($type == 'match' || $type == 'term' || $type == 'range') {

            foreach ($where_arr as $k => $v) {
                $this->combinedQuery['bool']['should'][] = [
                    $type => [$k => $v]
                ];
            }
        }
        $this->_search_params['body']['query'] = $this->combinedQuery;
        return $this;
    }

    /**
     * 排除条件
     */
    public function _where_not($where_arr, $type = 'match')
    {
        if ($type == 'match' || $type == 'term' || $type == 'range') {
            foreach ($where_arr as $k => $v) {
                $this->combinedQuery['bool']['must_not'][] = [
                    $type => [$k => $v]
                ];
            }
        }
        $this->_search_params['body']['query'] = $this->combinedQuery;
        return $this;
    }

    /**
     * @return int 查询条数
     */
    public function _get_search_nums()
    {
        return $this->_search_nums;
    }

    /**
     * @param $field_str  要显示字段
     * @param $is_excludes 是否排除掉要显示的字段
     *
     * @return $this
     */
    public function _field($field_str, $is_excludes = false)
    {
        if(empty($field_str)){
            return $this;
        }

        # 记录下字段 可能 groupby处会用到
        $this->_field_str = $field_str;

        //header("Content-Type: application/json");
        # ==========================================================
        $field_arr = explode(',', $field_str);
        $field_arr_org = [];
        $field_arr_as = [];

        foreach ($field_arr as $k => $v) {
            $v = trim($v);

            $alias_name = $this->getAlias($v);
            $org_name = $this->getOrgField($v);
            $this->_field_as_name[md5($org_name)] = $alias_name ? $alias_name : $org_name;

            # 判断下是否含有聚合函数
            $aggs_function_arr = $this->checkAggregateFunction($org_name);
            if (!empty($aggs_function_arr)) {

                # 含有聚合函数
                # 取到聚合方法 sum avg ...
                $aggs_function = strtolower(trim($aggs_function_arr[1]));
                $aggs_function = $this->mysql_mapping_to_es_aggs_function[$aggs_function];

                # 是否存在运算符号
                $has_operation = $this->aggsFunctionParamHasOperation($aggs_function_arr[2]);
                if (!$has_operation) {
                    $this->_search_params['body']['aggs'][md5($org_name)] =  [
                        $aggs_function => [
                            'field' => $aggs_function_arr[2]
                        ]
                    ];
                } else {
                    $this->_search_params['body']['aggs'][md5($org_name)] =  [
                        $aggs_function => [
                            'script' => $has_operation
                        ]
                    ];
                }

            } else {
                # 这里的 $org_item 没有聚合方法
                $field_arr_org[] = $org_name;
            }


        }

        $this->_search_params['body']['_source'] = $is_excludes == false ? $field_arr_org:["excludes"=> $field_arr_org];

        return $this;
    }

    public function _order_by($order_by_arr)
    {
        // todo 排序好像只能整型 text 类型的 排序会报错
        $new_order_by = [];
        foreach ($order_by_arr as $k => $v) {
            $new_order_by[] = [$k=>['order'=>$v]];
        }
        $this->_search_params['body']['sort'] = $new_order_by;//[["create_time" => ["order" => "desc"]]];
        return $this;
    }

    /**
     * @param $group_str 多个字段逗号隔开
     * @return $this
     */
    public function _group_by($group_str = '')
    {
        $this->_search_params['body']['size'] = 0;


        $group_arr = explode(',', $group_str);
        $aggs_a = $this->_search_params['body']['aggs'];
        $this->_search_params['body']['aggs'] = null;
        $this->_search_params['body']['size'] = 0;
        $this->_search_params['body']['aggs'] = $this->combination_aggs_params($group_arr,$aggs_a);

        return $this;
    }

    protected function combination_aggs_params($group_by_field,$aggs_){
        $item = trim(array_shift($group_by_field));

        return [
            'group_by_'.$item => [
                'terms' => [
                    'field' => $item,
                    'size'  => $this->_search_size,
                ],
                'aggs' => !empty($group_by_field) ? $this->combination_aggs_params($group_by_field,$aggs_):$aggs_
            ]
        ];

    }

    protected function combination_aggregations($p_buckets=[],$p_bucket_index=''){
        $new_array = [];
        foreach ($p_buckets as $bucket) {
            $has_sun_bucket    = false;
            $has_group_by_name = '';
            foreach ($bucket as $k => $v) {
                if (strpos($k, 'group_by_') !== false) {
                    $has_sun_bucket    = true;
                    $has_group_by_name = $k;
                }
            }

            if ($has_sun_bucket) {
                $temp2 = $this->combination_aggregations($bucket[$has_group_by_name]['buckets'], $has_group_by_name);
                foreach ($temp2 as $k => $v) {
                    $temp2[$k]   = [$p_bucket_index => $bucket['key']] + $temp2[$k];
                    $new_array[] = $temp2[$k];
                }

            } else {
                $temp = [];
                foreach ($bucket as $k => $v) {
                    if ($k == 'key') {
                        $temp[$p_bucket_index] = $v;
                    }
                    if ($k != 'key' && !empty($v['value'])) {
                        $temp[$k] = $v['value'];
                    }
                }
                $new_array[] = $temp;
            }
        }

        return $new_array;
    }

    public function _search_list()
    {

        try {
            $response_array = [];
            $response = $this->client->search($this->_search_params);
            $_field_as_keys = array_keys($this->_field_as_name);

            $aggregations = $response['aggregations'] ?? [];
            if (!empty($aggregations)) {

                foreach ($aggregations as $k => $v) {

                    if (!empty($v['buckets'])) {
                        $response_array = $this->combination_aggregations($v['buckets'], $k);
                    } else {
                        $response_array[$k] = $v['value'];
                    }
                }
                //echo json_encode($response_array,JSON_PRETTY_PRINT)."\n";die;
                # 修改字段名字 使用别名 没有的 直接显示 todo 后面可以整合下
                foreach ($response_array as $k => $row) {
                    $new_keys = [];

                    if (is_array($row)) {
                        foreach ($row as $kk => $vv) {
                            if (in_array($kk, $_field_as_keys)) {
                                $new_keys[] = $this->_field_as_name[$kk];
                            } else {
                                $new_keys[] = $kk;
                            }
                        }

                        $response_array[$k] = array_combine($new_keys, $response_array[$k]);

                    } else {

                        $new_key = $k;

                        if(!empty($response_array[$k])){
                            unset($response_array[$k]);
                        }

                        if (in_array($k, $_field_as_keys)) {
                            $new_key = $this->_field_as_name[$k];
                        }

                        $response_array[] = [$new_key => $row];
                    }

                }

                return $response_array;

            } else {

                # 正常查询
                $this->_search_nums = $response['hits']['total']['value'] ?? 0;


                foreach ($response['hits']['hits'] as $hit) {
                    $source = $hit['_source'];
                    $new_keys = [];
                    if (!empty($source)) {
                        foreach ($source as $k => $v) {
                            if (in_array($k, $_field_as_keys)) {
                                $new_keys[] = $this->_field_as_name[$k];
                            } else {
                                $new_keys[] = $k;
                            }
                        }
                        $source = array_combine($new_keys, $source);
                        $response_array[] = $source;
                    }
                }

                return $response_array;

            }
        } catch (\Exception $e) {
            throw new Exception("查询es 发生错误：" . $e->getMessage());
        }


    }

    public function _search_find()
    {

        try {
            $this->_search_params['body']['size'] = 1;
            $response = $this->client->search($this->_search_params);

            $response_array = [];
            foreach ($response['hits']['hits'] as $hit) {
                $source = $hit['_source'];
                if (!empty($source)) {
                    $temp = [];
                    foreach ($source as $k => $v) {
                        $temp[$k] = $v;
                    }
                    $response_array = $temp;
                }
            }
        } catch (\Exception $e) {
            throw new Exception("查询es 发生错误：" . $e->getMessage());
        }

        return $response_array;
    }

    /**
     * 检查聚合函数  拿到 聚合函数和聚合函数里的字段   eg: sum(age) => $res[1] = sum ，$res[2] = age
     */
    protected function checkAggregateFunction($value)
    {

        $pattern = '/\b(avg|sum|min|max|count)\((.*)\)/i';
        preg_match($pattern, $value, $res);
        return empty($res) ? false : $res;
    }

    /**
     * 获取字段别名
     * 如果没有别名就抛出 原字段 目前支持如下正则
     *
    ' column1 AS alias1 ',
    '  column1 alias1  ',
    'count(column1) as alias1',
    'count(column1) alias1',
    'count(column1 + column2) alias1',
    ' count(column1 + column2)',
    ' count(column1 + column2 )',
    'count(column1)',
    'column1',
    '  column1 21 ',
     *
     */
    protected function getAlias($value)
    {
        $field = '';
        // 去除 () 中的空白格
        $value = preg_replace('/\s(?=[^()]*\))/', '', $value);
        $pattern = '/(.*)(?(?=\))\)\s+\b(AS|as)?(.*)|\s+\b(AS|as)?(.*))/';
        preg_match($pattern, $value, $res);
        if (!empty($res)) {
            $field = array_pop($res);
        }
        return trim($field);
    }

    /**
     * @param $value  field的值  ddd as al
     *
     * @return string  ddd
     */
    protected function getOrgField($value)
    {
        $pattern = '/\b(\S*)/';
        $value = preg_replace('/\s(?=[^()]*\))/', '', $value);
        preg_match($pattern, $value, $res);
        return array_pop($res);
    }

    protected function aggsFunctionParamHasOperation($value)
    {

        $source_str = '';
        # 判断是否存在条件判断  $value = SUM(CASE WHEN amount > 4 THEN amount - user_id ELSE amount END) =>
        # CASE WHEN amount>4 THEN amount-user_id ELSE amount END => =>
        # CASEWHENamount>4THENamount-user_idELSEamountEND
        $has_condition = preg_match('/^CASEWHEN(.*)THEN(.*)ELSE(.*)END$/', $value, $res);
        if($has_condition){
            $if_con  = $res[1];  // 条件
            $if_then = $res[2];  // 条件成立时的操作
            $if_end  = $res[3];  // 条件失败时的操作
            if (empty($if_con) || empty($if_then) || empty($if_end)) {
                throw new Exception("语法错误!!!");
            }


            # 将mysql中使用的比较符 逻辑符号 换成 es 中用的
            $if_con = str_replace(array_keys($this->mysql_mapping_to_es_comparison), array_values($this->mysql_mapping_to_es_comparison), $if_con);
            $if_con = str_replace(array_keys($this->mysql_mapping_to_es_logical), array_values($this->mysql_mapping_to_es_logical), $if_con);

            $if_con = preg_replace_callback('/\b([a-zA-Z_]+)\s*/', function($matches){
                return "doc['" . $matches[1] . "'].value";

            }, $if_con);

            $if_then = preg_replace_callback('/\b([a-zA-Z_]+)\s*/', function($matches){
                return "doc['" . $matches[1] . "'].value";

            }, $if_then);

            $if_end = preg_replace_callback('/\b([a-zA-Z_]+)\s*/', function($matches){
                return "doc['" . $matches[1] . "'].value";

            }, $if_end);

            $source_str = $if_con . " ? " . $if_then . " : " . $if_end;

            return [
                'source' => $source_str,
            ];

        }


        # 判断是否存在 运算因子 + - * /
        $has_operation = preg_match('/(.*)[+\-*\/]/', $value, $res);
        if (!$has_operation) {
            return false;
        }

        $source_str = preg_replace_callback('/\b([a-zA-Z_]+)\s*/', function($matches){
            return "doc['" . $matches[1] . "'].value";

        }, $value);

        return [
            'source' => $source_str,
        ];
    }
}