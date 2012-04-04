<?php

/**
 *
 * SQL Builder for generating MySQL Select SQL
 * Modified From https://github.com/c9s/SQLBuilder
 *
 * @code
 *
 *  $sqlbuilder = new QueryBuilder();
 *
 *  $sqlbuilder->table('fact.`order`', 'f')
 *    ->select(array(
 *          'quantity' => 'summary(quantity)',
 *          'averageprice' => 'ifnull(sum(revenue) / sum(quantity), 0)')
 *    ->join('dim.date', 'd', '', 'f.date = d.date')
 *    ->where('d.date between ? and ?', 'ss', array($begin_date, $end_date))
 *    ->groupby('d.month_num_overall')
 *    ->orderby('d.month_num_overall desc')
 *
 *  $sql = $sqlbuilder->build();
 *
 * @code
 * [TODO]
 * 1. subquery with bindparam [2012.3.20 done]
 * 2. param placeholder
 */
class QueryBuilder
{
    /**
     * selected columns
     *
     * @var string[] an array contains column names
     */
    public $selects = array();

    /**
     * table name
     *
     * @var string
     * */
    public $table;

    /**
     * table alias
     */
    public $alias;

    public $joins = array();

    public $wheres = array();

    /**
     * for prepared statements
     * 'scope' => array('type', 'params')
     */
    public $params_dict = array();

    const S_TABLE = 'table';
    const S_JOIN = 'join';
    const S_WHERE = 'where';
    const S_HAVING = 'having';

    public $groupbys = array();

    public $havings = array();

    public $orderbys = array();

    /**
     * limit
     *
     * @var integer
     * */
    public $limit;

    /**
     * offset attribute
     *
     * @var integer
     * */
    public $offset;


    public function __construct()
    {
    }

    /**
     * set table name
     *
     * @param string $table table name
     */
    public function table($table, $alias = null)
    {
        $subquery = $this->isSubQuery($table);
        if ($subquery) {
            assert(!is_null($alias));
            $this->table = ' ( '. $table->build(). ' ) ';
            list($type, $params) = $table->buildParam();
            $this->addParam(self::S_TABLE, $type, $params);
        } else {
            $this->table = $table;
        }
        $this->alias = $alias;
        return $this;
    }

    /**
     * select behavior
     * @param Array($alias => $exp, $exp) $columns
     * @param array
     */
    public function select($columns)
    {
        foreach ($columns as $alias => $exp) {
            if( is_string($alias) ) {
                $this->selects[$alias] = $exp;
            }
            elseif( is_integer($alias) ) {
                $this->selects[$exp] = $exp;
            }
        }
        return $this;
    }

    /**
     * join table
     *
     * @param string $table table name
     * @param string $alias table alias
     * @param string $type  join type, valid types are: 'left', 'right', 'inner' ..
     * @param string $on    join on expression
     */
    public function join($table, $alias, $type = null, $on = null)
    {
        $subquery = $this->isSubQuery($table);

        $sql = ' ' . $type . ' JOIN ';

        if ($subquery) {
            assert(!is_null($alias));
            $sql .= ' ( '. $table->build(). ' ) ' ;
            list($type, $params) = $table->buildParam();
            $this->addParam(self::S_JOIN, $type, $params);
        } else {
            $sql .= $table;
        }

        if( $alias )
            $sql = $sql . ' ' . $alias;

        if ($on)
            $sql .= ' ON ' . $on;

        $this->joins[] = $sql;

        return $this;
    }


    /**
     * setup where condition
     * @param string $exp condition expression
     * @param string $type
     * @param string $params
     *
     */
    public function where($exp, $type = null, $params = null)
    {
        $this->wheres[] = $exp;

        if ($type) {
            $this->addParam(self::S_WHERE, $type, $params);
        }

        return $this;
    }

    /**
     * group by column
     *
     * @param string $column... column name
     */
    public function groupby($column)
    {
        $args = func_get_args();
        if( count($args) > 1 ) {
            $this->groupbys = array_merge($this->groupbys, $args);
        } else {
            $this->groupbys[] = $column;
        }
        return $this;
    }

    /**
     * group by column
     *
     * @param Array($col, ...) $columns
     */
    public function groupbyArr($columns)
    {
        $this->groupbys = array_merge($this->groupbys, $columns);
        return $this;
    }

    /**
     * having
     */
    public function having($exp, $type = null, $params = null)
    {
        $this->havings[] = $exp;

        if ($type) {
            $this->addParam(self::S_HAVING, $type, $params);
        }

        return $this;
    }

    /**
     * push order
     *
     * @param string $column column name with 'desc or asc'
     */
    public function orderby($column)
    {
        $args = func_get_args();
        if( count($args) > 1 ) {
            $this->orderbys = array_merge($this->orderbys, $args);
        } else {
            $this->orderbys[] = $column;
        }
        return $this;
    }

    /**
     * push order
     *
     * @param Array(string $col, ...) $columns
     * @comment col = column name with 'desc or asc'
     */
    public function orderbyArr($columns)
    {
        $this->orderbys = array_merge($this->orderbys, $columns);
        return $this;
    }

    /**
     * setup limit syntax
     *
     * @param integer $limit
     */
    public function limit($limit)
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * setup offset syntax
     *
     * @param integer $offset
     */
    public function offset($offset)
    {
        $this->offset = $offset;
        return $this;
    }

    public function build()
    {
        $sql = 'SELECT ' . $this->buildSelectColumns() . ' FROM ' . $this->getTableSql() . ' ';

        $sql .= $this->buildJoinSql();

        $sql .= $this->buildConditionSql();

        $sql .= $this->buildGroupBySql();

        $sql .= $this->buildHavingSql();

        $sql .= $this->buildOrderSql();

        $sql .= $this->buildLimitSql();

        return $sql;
    }

    public function getBindParams()
    {
        list($type, $params) = $this->buildParam();
        return array_merge(array($type), $params);
    }

    public function getReturnParams()
    {
        return array_keys($this->selects);
    }

    #=================protect method==================

    protected function addParam($scope, $type, $params)
    {
        if (!isset($this->params_dict[$scope])) {
            $this->params_dict[$scope] = array('type' => '', 'params' => array());
        }

        assert(is_string($type));
        assert(is_array($params));
        assert(count($params) === strlen($type));

        $this->params_dict[$scope]['type'] .= $type;
        $this->params_dict[$scope]['params'] = array_merge($this->params_dict[$scope]['params'], $params);
    }

    protected function buildSelectColumns()
    {
        $cols = array();
        foreach( $this->selects as $k => $v ) {
           $cols[] = $v !== $k ? $v . '  AS ' . $k : $v;
        }
        return join(', ',$cols);
    }

    protected function getTableSql()
    {
        $sql = '';
        $sql .= $this->table;
        if( $this->alias )
            $sql .= ' ' . $this->alias;
        return $sql;
    }

    protected function buildJoinSql()
    {
        return join('', $this->joins);
    }

    protected function buildConditionSql()
    {
        $sql = '';
        if ($this->wheres) {
            $sql = ' WHERE '. join(' AND ', $this->wheres);
        }
        return $sql;
    }

    protected function buildGroupBySql()
    {
        if( ! empty($this->groupbys) ) {
            return ' GROUP BY ' . join( ',' , $this->groupbys);
        }
    }

    protected function buildHavingSql()
    {
        $sql = '';
        if ($this->havings) {
            $sql = ' HAVING '. join(' AND ', $this->havings);
        }
        return $sql;
    }

    protected function buildOrderSql()
    {
        if( ! empty($this->orderbys) ) {
            return ' ORDER BY ' . join( ',' , $this->orderbys);
        }
    }

    protected function buildLimitSql()
    {
        $sql = '';
        if( $this->limit && $this->offset ) {
            $sql .= ' LIMIT ' . $this->offset . ' , ' . $this->limit;
        } else if ( $this->limit ) {
            $sql .= ' LIMIT ' . $this->limit;
        }
        return $sql;
    }

    protected function isSubQuery($table)
    {
       return (gettype($table) == 'object' && get_class($table) === get_class($this));
    }

    protected function buildParam()
    {
        $type = '';
        $params = array();
        $list = array(self::S_TABLE, self::S_JOIN, self::S_WHERE, self::S_HAVING);
        foreach ($list as $scope) {
            if (isset($this->params_dict[$scope])) {
                $ret = $this->params_dict[$scope]; 
                $type .= $ret['type'];
                $params = array_merge($params, $ret['params']);
            }
        }
        return array($type, $params);
    }
}

