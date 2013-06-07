<?php
namespace blackbass\sphinx;


/**
 * Class SphinxProvider
 *
 * @package blackbass\sphinx
 */
class SphinxProvider {
    /**
     * query type "enum"
     */
    const FILTER_INCLUDE = 0;
    const FILTER_EXCLUDE = 1;
    const RANGE_INCLUDE = 2;
    const RANGE_EXCLUDE = 3;
    const SORT_ASC = 4;
    const SORT_DESC = 5;
    const LT_LIMIT = 6;
    const LT_OFFSET = 7;
    const LT_CUTOFF = 8;
    const SELECT = 9;
    const MAX_MATCHES = 10;
    const MODE = 11;
    const GROUP_BY = 12;

    const SORT_ASC_STR = 'asc';
    const SORT_DESC_STR = 'desc';
    const MODE_EXTENDED2 = 'extended2';
    const MODE_ANY = 'any';
    const SORT_RELEVANCE_STR = '@relevance';
    const SORT_ID_STR = '@id';

    const CONNECTION_TIMEOUT = 3;
    const MAX_QUERY_TIME = 0;

    private $fulltextQuery;
    private $queryArray;
    private $indexes = "*";
    private $conditionQuery = "";
    private $groupField = "";


    /**
     * @var \SphinxClient
     */
    private $sphinxApiClient = null;
    private $sphinxHost = null;
    private $pool;

    /**
     * @return \SphinxClient
     */
    public function getConnection() {
        return $this->sphinxApiClient;
    }

    /**
     * @param $sphinxConnection
     */
    public function setConnection($sphinxConnection) {
        $this->sphinxApiClient = $sphinxConnection;
    }

    /**
     * construct
     */
    function  __construct($pool = array())
    {
        $this->queryArray = array();
        $this->pool = $pool;
    }

    function __destruct()
    {
        if ($this->sphinxApiClient != null) {
            $this->sphinxApiClient->Close();
        }
    }

    /**
     * @return string
     */
    function __toString()
    {
        return $this->createSearchQuery();
    }

    /**
     * @return array
     */
    function __getQueryArray()
    {
        return $this->queryArray;
    }


    /**
     * generate main search query
     *
     * @return string
     */
    private function createSearchQuery()
    {
        $query = array();
        $sorts = array();
        $selects = array();
        $limits = array();
        $mode = "";
        foreach ($this->queryArray as $queryPart) {
            $queryPart['value'] = $this->normalizeValue($queryPart['value']);
            switch ($queryPart['type']) {
                case self::FILTER_INCLUDE:
                    $query[] = 'filter=' . $queryPart['name'] . ',' . $queryPart['value'];
                    break;
                case self::FILTER_EXCLUDE:
                    $query[] = '!filter=' . $queryPart['name'] . ',' . $queryPart['value'];
                    break;
                case self::RANGE_INCLUDE:
                    $query[] = 'range=' . $queryPart['name'] . ',' . $queryPart['value'];
                    break;
                case self::RANGE_EXCLUDE:
                    $query[] = '!range=' . $queryPart['name'] . ',' . $queryPart['value'];
                    break;
                case self::SORT_ASC:
                    $sorts[] = $queryPart['name'] . " ASC";
                    break;
                case self::SORT_DESC:
                    $sorts[] = $queryPart['name'] . " DESC";
                    break;
                case self::LT_LIMIT:
                    $limits[] = 'limit=' . $queryPart['value'];
                    break;
                case self::LT_OFFSET:
                    $limits[] = 'offset=' . $queryPart['value'];
                    break;
                case self::LT_CUTOFF:
                    $limits[] = 'cutoff=' . $queryPart['value'];
                    break;
                case self::SELECT:
                    $selects[] = $queryPart['value'];
                    break;
                case self::MAX_MATCHES:
                    $query[] = 'maxmatches=' . $queryPart['value'];
                    break;
                case self::MODE:
                    $mode = 'mode=' . $queryPart['value'];
                    break;
                case self::GROUP_BY:
                    switch ($queryPart['value']) {
                        case SPH_GROUPBY_ATTR:
                            $groupText = 'attr';
                            break;
                        case SPH_GROUPBY_DAY:
                            $groupText = 'day';
                            break;
                        case SPH_GROUPBY_WEEK:
                            $groupText = 'week';
                            break;
                        case SPH_GROUPBY_MONTH:
                            $groupText = 'month';
                            break;
                        case SPH_GROUPBY_YEAR:
                            $groupText = 'year';
                            break;
                        default:
                            $groupText = 'year';
                            break;
                    }
                    $query[] = "groupby={$groupText}:{$queryPart['name']}";
                    $query[] = "groupsort={$queryPart['group_sort']}";
                    break;
            }

        }

        $whereQuery = '';
        if (!empty($this->fulltextQuery)) {
            $whereQuery = preg_replace('/\;|\=|\\|\ $/', '', $this->fulltextQuery) . ';';
            $mode = 'mode=extended2';
            $sorts[] = self::SORT_RELEVANCE_STR . ' DESC';
        }
        if (!empty($this->indexes)) {
            $whereQuery .= "index={$this->indexes};";
        }
        if (sizeof($selects)) {
            $whereQuery .= 'select=*,' . join(', ', $selects) . ';';
        }

        if (sizeof($sorts) > 0) {
            $whereQuery .= 'sort=extended:' . join(', ', $sorts) . ';';
        }
        if (!empty($mode)) {
            $whereQuery .= $mode . ';';
        }
        $whereQuery .= join(';', $query) . ';';
        if (sizeof($limits) > 0) {
            $whereQuery .= join(';', $limits);
        }

        return $whereQuery;
    }

    /**
     * generate query based on conditions
     *
     * @deprecated
     * @return string
     */
    public function getSphinxQuery()
    {
        $this->conditionQuery = $this->createSearchQuery();
        return $this->conditionQuery;
    }

    /**
     * @param string|array          $filter
     * @param integer|array         $filterCondition
     * @param bool                  $isExclude
     * @param bool                  $isWeak drops condition on $this->clearWeakConditions
     */
    public function addFilter($filter, $filterCondition, $isExclude = false, $isWeak = false)
    {
        if (is_array($filter)) {
            foreach ($filter as $f) {
                $this->addFilter($f, $filterCondition);
            }
        } else {
            if (is_array($filterCondition)) {
                $filterConditionArray = $filterCondition;
                $filterCondition = implode(',', $filterCondition);
            } else {
                $filterConditionArray = array($filterCondition);
            }
            if (!count($filterCondition)) {
                return;
            }
            $this->queryArray[] = array(
                "type"       => $isExclude ? self::FILTER_EXCLUDE : self::FILTER_INCLUDE,
                "name"       => $filter,
                "value"      => $filterCondition,
                "valueArray" => $filterConditionArray,
                "isWeak"     => $isWeak
            );
        }
    }

    /**
     * example: addRange(foo,1,) means foo>=1;
     * addRange(foo,,1) means foo<=1;
     * addRange(foo,1,2) means foo>=1 and foo<=2
     *
     * @param            $filter
     * @param int|string $start
     * @param int|string $stop
     * @param bool       $isExclude
     * @param bool       $isWeak
     */
    public function addRange($filter, $start = 0, $stop = PHP_INT_MAX, $isExclude = false, $isWeak = false)
    {
        if (!empty($start) || !empty($stop)) {
            if ($start == $stop) {
                $this->addFilter($filter, $start);
            } else {
                $filterCondition = $start . ',' . $stop;
                $this->queryArray[] = array(
                    "type"   => $isExclude ? self::RANGE_EXCLUDE : self::RANGE_INCLUDE,
                    "name"   => $filter,
                    "value"  => $filterCondition,
                    "min"    => $start,
                    "max"    => $stop,
                    "isWeak" => $isWeak
                );
            }
        }
    }

    /**
     * @param array|string  $field
     * @param string        $direction
     * @param bool          $isWeak
     * @param bool          $priority
     */
    public function addSort($field, $direction, $isWeak = false, $priority = false)
    {
        if (is_array($field)) {
            foreach ($field as $f) {
                $this->addSort($f, $direction, $isWeak, $priority);
            }
        } else {
            $sort = null;
            switch ($direction) {
                case 'asc':
                    $sort = array(
                        'type'   => self::SORT_ASC,
                        'name'   => $field,
                        'isWeak' => $isWeak,
                        'value'  => null
                    );
                    break;
                case 'desc':
                    $sort = array(
                        'type'   => self::SORT_DESC,
                        'name'   => $field,
                        'isWeak' => $isWeak,
                        'value'  => null
                    );
                    break;
            }
            if ($priority) {
                array_unshift($this->queryArray, $sort);
            } else {
                $this->queryArray[] = $sort;
            }
        }
    }

    public function addMaxMatches($max, $isWeak = false)
    {
        $this->queryArray[] = array(
            'type'   => self::MAX_MATCHES,
            'value'  => $max,
            'isWeak' => $isWeak
        );
    }

    /**
     * @param int  $limit
     * @param int  $offset
     * @param int  $cutoff
     * @param bool $isWeak
     */
    public function addLimit($limit = 20, $offset = 0, $cutoff = 0, $isWeak = false)
    {
        if ($offset) {
            $this->queryArray[] = array(
                'type'   => self::LT_OFFSET,
                'value'  => $offset,
                'isWeak' => $isWeak
            );
        }
        if ($cutoff) {
            $this->queryArray[] = array(
                'type'   => self::LT_CUTOFF,
                'value'  => $cutoff,
                'isWeak' => $isWeak
            );
        }
        if ($limit) {
            $this->queryArray[] = array(
                'type'   => self::LT_LIMIT,
                'value'  => $limit,
                'isWeak' => $isWeak
            );
        }
    }

    /**
     * @param string|array $fullTextSearchValue
     */
    public function appendFullTextSearch($fullTextSearchValue)
    {
        if ($this->fulltextQuery == null) {
            $this->fulltextQuery = '';
        }
        if (is_array($fullTextSearchValue)) {
            foreach ($fullTextSearchValue as $t) {
                $this->appendFullTextSearch($t);
            }
        } else {
            $this->fulltextQuery .= $fullTextSearchValue . ' ';
        }
    }

    /**
     * @param      $selectQuery
     * @param bool $isWeak
     */
    public function addSelect($selectQuery, $isWeak = false)
    {
        if (is_array($selectQuery)) {
            foreach ($selectQuery as $q) {
                $this->addSelect($q, $isWeak);
            }
        } else {
            if (!empty($selectQuery)) {
                $this->queryArray[] = array(
                    'type'   => self::SELECT,
                    'value'  => $selectQuery,
                    'isWeak' => $isWeak
                );
            }
        }
    }

    /**
     * @param string|array $indexVariants
     */
    public function setIndex($indexVariants = '*')
    {
        if (empty($indexVariants)) {
            $indexVariants = '*';
        }
        if (is_array($indexVariants)) {
            $this->indexes = join(',', $indexVariants);
        } else {
            $this->indexes = $indexVariants;
        }
    }

    /**
     * @param      $mode
     * @param bool $isWeak
     */
    public function setMode($mode, $isWeak = false)
    {
        $this->queryArray[] = array(
            'type'   => self::MODE,
            'value'  => $mode,
            'isWeak' => $isWeak
        );
    }


    /**
     * @param $attribute
     * @param $sortType
     * @param $groupSort
     * @param $isWeak
     */
    public function setGroupBy($attribute, $sortType, $groupSort = '@group desc', $isWeak = false) {
        $this->groupField = $attribute;
        $this->addSelect($attribute);
        $this->queryArray[] = array(
            'type'   => self::GROUP_BY,
            'name'  => $attribute,
            'value'  => $sortType,
            'group_sort' => $groupSort,
            'isWeak' => $isWeak
        );
    }

    /**
     * clear all weak conditions
     */
    public function clearWeakConditions()
    {
        $conditionsCopy = array();
        foreach ($this->queryArray as $condition) {
            if (!$condition['isWeak']) {
                $conditionsCopy[] = $condition;
            }
        }
        $this->queryArray = $conditionsCopy;
        unset($conditionsCopy);
    }

    /**
     * @deprecated
     * @return array
     */
    public function getQueryArray()
    {
        return $this->queryArray;
    }

    /**
     * @param string $mainTableAlias
     * @param string $idField
     * @param int    $ids
     * @param bool   $recreateConditions
     *
     * @return string
     */
    public function getWhereIdWithOrderFunction(
        $mainTableAlias = "s", $idField = "id", $ids = 0, $recreateConditions = false
    ) {
        if (empty($this->conditionQuery) || $recreateConditions) {
            $this->conditionQuery = $this->createSearchQuery();
        }

        return " where $mainTableAlias.$idField in ($ids) order by field($mainTableAlias.$idField, $ids)";
    }

    /**
     * @return bool
     */
    public static function isSphinxAlive()
    {
//        sphinx status gets from servers and configuration locates in memcache
//        return memcache_get("SPHINX_FATAL_DIED");
        return true;
    }

    /**
     * It will be round robing mechanism
     *
     * @return array
     */
    private function getSphinxHost()
    {
        $pool = null;
//        sphinx status gets from servers and configuration locates in memcache
//        $pool = memcache_get("SPHINX_STATUS_LIVE_HOSTS");
        if ($pool == null) {
            $pool = $this->pool;
        }
        $host = $pool[rand(0, sizeof($pool) - 1)];
        return $host;
    }

    /**
     * @param $value
     *
     * @return mixed
     */
    private function normalizeValue($value)
    {
        $value = preg_replace('/\;.+$/', '', $value);
        return $value;
    }

    /**
     * create sphinx persistence connection
     */
    public function makeConnection()
    {
        if ($this->sphinxApiClient == null) {
            $this->sphinxApiClient = new \SphinxClient();
            $host = $this->getSphinxHost();
            $this->sphinxHost = $host['host'];
            $this->sphinxApiClient->SetConnectTimeout(self::CONNECTION_TIMEOUT);
            $this->sphinxApiClient->SetMaxQueryTime(self::MAX_QUERY_TIME);

            $this->sphinxApiClient->SetServer($host['host'], $host['port']);
            $this->sphinxApiClient->Open();
        }
    }

    /**
     * @return \SphinxClient
     */
    private function _doApiRequest()
    {
        $this->makeConnection();
        $client = $this->sphinxApiClient;
        $client->ResetFilters();
        $client->ResetGroupBy();

        $sorts = array();
        $selects = array();

        $offset = 0;
        $limit = 0;
        $cutoff = 0;
        $max = 20;

        foreach ($this->queryArray as $queryPart) {
            $queryPart['value'] = $this->normalizeValue($queryPart['value']);
            switch ($queryPart['type']) {
                case self::FILTER_INCLUDE:
                    $client->SetFilter($queryPart['name'], $queryPart['valueArray']);
                    break;
                case self::FILTER_EXCLUDE:
                    $client->SetFilter($queryPart['name'], $queryPart['valueArray'], true);
                    break;
                case self::RANGE_INCLUDE:
                    $client->SetFilterRange($queryPart['name'], $queryPart['min'], $queryPart['max']);
                    break;
                case self::RANGE_EXCLUDE:
                    $client->SetFilterRange($queryPart['name'], $queryPart['min'], $queryPart['max'], true);
                    break;
                case self::SORT_ASC:
                    $sorts[] = $queryPart['name'] . " ASC";
                    break;
                case self::SORT_DESC:
                    $sorts[] = $queryPart['name'] . " DESC";
                    break;
                case self::LT_LIMIT:
                    $limit = (int)$queryPart['value'];
                    break;
                case self::LT_OFFSET:
                    $offset = (int)$queryPart['value'];
                    break;
                case self::LT_CUTOFF:
                    $cutoff = (int)$queryPart['value'];
                    break;
                case self::SELECT:
                    $selects[] = $queryPart['value'];
                    break;
                case self::MAX_MATCHES:
                    $max = $queryPart['value'];
                    break;
                case self::MODE:
                    $client->SetMatchMode(SPH_MATCH_EXTENDED2);
                    break;
                case self::GROUP_BY:
                    $client->SetGroupBy($queryPart['name'], $queryPart['value'], $queryPart['group_sort']);

            }

        }

        if (!empty($this->fulltextQuery)) {
            $this->fulltextQuery = preg_replace('/\;|\=|\\|\ +}$/', '', $this->fulltextQuery) . ';';
            $client->SetMatchMode(SPH_MATCH_EXTENDED2);
            $sorts[] = self::SORT_RELEVANCE_STR . ' DESC';
        }

        if (sizeof($selects)) {
            $client->SetSelect(join(', ', $selects));
        }

        if (sizeof($sorts) > 0) {
            $client->SetSortMode(SPH_SORT_EXTENDED, join(', ', $sorts));
        }
        $client->SetLimits($offset, $limit, $max, $cutoff);

        return $client;
    }


    /**
     * @param string $attribute
     * @param string $sort
     *
     * @param array  $additionalFields
     * @return array
     */
    public function getGroupRequestFromApi($attribute, $sort = '@count desc', $additionalFields = null) {

        $this->setMode(SPH_MATCH_EXTENDED2);
        $this->addMaxMatches(2000);
        $this->addLimit(2000);
        $this->addSelect("@count");
        if ($additionalFields !== null) {
            foreach ($additionalFields as $additionalField) {
                $this->addSelect($additionalField);
            }
        }

        $this->setGroupBy($attribute, SPH_GROUPBY_ATTR, $sort);

        $error = '';

        $output = null;
//        $query = $this->getSphinxQuery();
//        $memcacheKey = 'SPHINX_' . md5($query);
//        $output = memcache_get($memcacheKey);
        if ($output == null) {
            $output = array(
                'data' => array(),
                'total' => 0,
            );
            $client = $this->_doApiRequest();
            $result = $client->Query($this->fulltextQuery, $this->indexes);

            if ($client->GetLastError() || $client->GetLastWarning()) {
                $error = $client->GetLastError() . $client->GetLastWarning();
                $output['err'] = $error;
            }

            $output['total_found'] = $result['total'];
            $output['time'] = $result['time'];
            foreach ($result['matches'] as $key => $value) {

                $newValue = array(
                    'id' => $value['attrs'][$this->groupField],
                    'cnt' => $value['attrs']['@count']
                );
                if ($additionalFields !== null) {
                    foreach ($additionalFields as $target=>$additionalField) {
                        if (is_string($target)) {
                            $newValue[$target] = $value['attrs'][$additionalField];
                        } else {
                            $newValue[$additionalField] = $value['attrs'][$additionalField];
                        }
                    }

                }
                $output['data'][] = $newValue;
            }
            $output['total'] = sizeof($output['data']);
            if (!$error) {
//                memcache_set($memcacheKey, $output, false, 60);
            }
        }

        return $output;
    }

    /**
     * @param array $additionalFields
     *
     * @return array|bool|mixed|string
     */
    public function doApiRequest($additionalFields = array())
    {
        $output = null;
//        $query = $this->getSphinxQuery();
//        $queryKey = 'SPHINX_' . md5($query);
//        $output = memcache_get($queryKey);

        if ($output == null) {

            if ($additionalFields !== null) {
                foreach ($additionalFields as $additionalField) {
                    $this->addSelect($additionalField);
                }
            }
            $client = $this->_doApiRequest();

            $result = $client->Query($this->fulltextQuery, $this->indexes);

            $output = array(
                'data'        => array(0),
                'total'       => 0,
                'idsByOrder'  => '0',
                'total_found' => $result['total'],
                'warnings'    => $result['warnings'],
                'errors'      => $result['errors'],
                'time'      => $result['time']
            );
            $error = null;
            if ($client->GetLastError()) {
                $error = $client->GetLastError();
            } elseif ($client->GetLastWarning()) {
                $error = $client->GetLastWarning();
            } else {
                $output['data'] = array();
                foreach ($result['matches'] as $key => $value) {
                    $newValue = array(
                        'id' => $key,
                    );
                    if ($additionalFields !== null) {
                        foreach ($additionalFields as $target=>$additionalField) {
                            if (is_string($target)) {
                                $newValue[$target] = $value['attrs'][$additionalField];
                            } else {
                                $newValue[$additionalField] = $value['attrs'][$additionalField];
                            }
                        }
                    }
                    $output['data'][$key] = $newValue;
                }
                $output['total'] = sizeof($output['data']);
                $output['idsByOrder'] = implode(',', array_keys($output['data']));
                if (sizeof($output['data']) == 0) {
                    $output['data'] = array(0);
                    $output['idsByOrder'] = '0';
                }
//                memcache_set($queryKey, $output, null, 60);
            }
            if (!empty($error)) {
                $output['err'] = $error;
            }
        }

        return $output;
    }

    /**
     * @return bool
     */
    public static function useSphinxInSearch() {
        return self::isSphinxAlive();
    }

}