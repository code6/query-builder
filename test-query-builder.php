<?php

require_once  'my-query-builder.php';

class QueryBuilderTest extends PHPUnit_Framework_TestCase
{
    function testCase1()
    {
        $sqlbuilder = new QueryBuilder();

        $begin_date = '20120315';
        $end_date = '20120315';

        $sqlbuilder->table('fact.`order`', 'f')
          ->select(array(
                'city',
               'quantity' => 'sum(quantity)',
               'averageprice' => 'ifnull(sum(revenue) / sum(quantity), 0)'))
          ->join('dim.date', 'd', '', 'f.date = d.date')
          ->where('d.date between ? and ?', 'ss', array($begin_date, $end_date))
          ->groupby('d.month_num_overall')
          ->groupbyArr(array('d.day_num_overall'))
          ->orderby('d.month_num_overall desc')
          ->orderbyArr(array('d.month_num_overall asc'))
          ->limit(3)
          ->offset(3);

        $expect = 'SELECT city, sum(quantity)  AS quantity, ifnull(sum(revenue) / sum(quantity), 0)  AS averageprice FROM fact.`order` f   JOIN dim.date d ON f.date = d.date WHERE d.date between ? and ? GROUP BY d.month_num_overall,d.day_num_overall ORDER BY d.month_num_overall desc,d.month_num_overall asc LIMIT 3 , 3';

        $this->assertEquals($sqlbuilder->build(), $expect);

        $expect = array('20120315', '20120315');

        $expect = array('ss', '20120315', '20120315');

        $this->assertEquals($sqlbuilder->getBindParams(),  $expect);

        $expect = array(
            'city',
            'quantity',
            'averageprice'
            );
        $this->assertEquals($sqlbuilder->getReturnParams(), $expect);
    }

    function testSubQuery()
    {
        $sql = "
            SELECT
                base.focus,
               truncate(1 - 1.0 / count(*) * (2 * sum(base.accumulate_revenue) / max(base.accumulate_revenue) -1), 2) g
            FROM
              (SELECT @cs := CASE
                                 WHEN @focus != raw.focus THEN raw.revenue
                                 ELSE @cs + raw.revenue
                             END accumulate_revenue, @focus := raw.focus AS focus
               FROM
                 (SELECT @cs := 0) cs_idx,
                 (SELECT @focus:='') s_idx,
                 ( SELECT f.FOCUS AS focus,
                          f.revenue
                  FROM fact.`order` f
                  WHERE  f.enddate BETWEEN ? AND ?
                    AND coupontype != 4
                  ORDER BY focus,
                           f.revenue) raw) base
            GROUP BY base.focus;
            ";

        $begin_date = '20120315';
        $end_date = '20120315';


        $sub1 = new QueryBuilder();
        $sub1->table('fact.`order`', 'f')
          ->where('d.date between ? and ?', 'ss', array($begin_date, $end_date))
          ->where ('coupontype != 4')
          ->orderby('focus', 'f.revenue');

        $sub0 = new QueryBuilder();
        $sub0->table(" (select @cs := 0) ", 'cs_idx')
          ->join(" (select @focus:='') ", "s_idx")
          ->join($sub1, 'raw')
          ->select(array('accumulate_revenue' => '@cs := CASE WHEN @focus != raw.focus THEN raw.revenue ELSE @cs + raw.revenue END',
                      'focus' => '@focus := raw.focus'));

        $gini  = new QueryBuilder();
        $gini->table($sub0, 'base')
          ->select(array(
                'focus',
                'g' => 'truncate(1 - 1.0 / count(*) * (2 * sum(base.accumulate_revenue) / max(base.accumulate_revenue) -1), 2)'))
          ->groupby('base.focus');

        $expect="SELECT focus, truncate(1 - 1.0 / count(*) * (2 * sum(base.accumulate_revenue) / max(base.accumulate_revenue) -1), 2)  AS g FROM  ( SELECT @cs := CASE WHEN @focus != raw.focus THEN raw.revenue ELSE @cs + raw.revenue END  AS accumulate_revenue, @focus := raw.focus  AS focus FROM  (select @cs := 0)  cs_idx   JOIN  (select @focus:='')  s_idx  JOIN  ( SELECT  FROM fact.`order` f  WHERE d.date between ? and ? AND coupontype != 4 ORDER BY focus,f.revenue )  raw )  base  GROUP BY base.focus";

        $this->assertEquals($gini->build(), $expect);

        $expect = array('ss', '20120315', '20120315');

        $this->assertEquals($gini->getBindParams(),  $expect);

        $sql_test_param = new QueryBuilder();
        $sql_test_param->table($sub0, 'base')
                       ->having('cnt > ?', 'i', array(2))
                       ->where('1 = ?', 'i', array(1))
                       ->groupby('base.focus')
                       ->select(array('focus',  'cnt' => 'count(*)'));

        $expect = "SELECT focus, count(*)  AS cnt FROM  ( SELECT @cs := CASE WHEN @focus != raw.focus THEN raw.revenue ELSE @cs + raw.revenue END  AS accumulate_revenue, @focus := raw.focus  AS focus FROM  (select @cs := 0)  cs_idx   JOIN  (select @focus:='')  s_idx  JOIN  ( SELECT  FROM fact.`order` f  WHERE d.date between ? and ? AND coupontype != 4 ORDER BY focus,f.revenue )  raw )  base  WHERE 1 = ? GROUP BY base.focus HAVING cnt > ?";

        $this->assertEquals($sql_test_param->build(), $expect);

        $expect = array('ssii', '20120315', '20120315', 1, 2);

        $this->assertEquals($sql_test_param->getBindParams(), $expect);
    }
}
