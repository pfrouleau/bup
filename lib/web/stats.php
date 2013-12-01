<html>
<head>
    <title>bup stats</title>
<style type="text/css">
body{font-family:monospace;}
td.right{text-align: right;}
</style>
</head>
<body>

<?php

class MyDB extends SQLite3
{
    function __construct()
    {
        $this->open('n:/Temp/50GB-VHD/patched-fanout-16.bup/bupstats-partial.sqlite3');
        echo "Database opened<p>";
    }

    function show_tree_size()
    {
        $R = $this->query('
            SELECT count(r_id) as c
            FROM refs
            GROUP BY r_id
            ORDER BY c');
        print "<table>";
        print "<th>
            <td>count</td>
            <td>nodes</td>
            </th>";
        $N = 1;
        $count = 0;
        while($row = $R->fetchArray())
        {
            if($row[0] > $N)
            {
                print "<tr>
                    <td class=right>{$count}</td><td>trees with</td>
                    <td class=right>{$N}</td><td>nodes</td>
                    </tr>";
                $N = $row[0];
                $count = 0;
            }
            $count += 1;
        }
        print "<tr>
            <td class=right>{$count}</td><td>trees with</td>
            <td class=right>{$N}</td><td>nodes</td>
            </tr>";
        print "</table>";

        $R = $this->query('
            SELECT o.sha, count(r.r_id) as c
            FROM refs r
            JOIN objects o
            WHERE r.r_id = o.id
            GROUP BY r.r_id
            ORDER BY c DESC, o.sha');
        print "<table>
               <tr><th>hash</th><th width=30px>childs</th></tr>";
        while ($row = $R->fetchArray())
        {
            if ($row[1]<100) break;
            print "<tr><td>{$row[0]}</td><td class=right>{$row[1]}</td></tr>";
        }
        print "</table>";
    }
}

function sqlite_query($dbhandle, $query) 
{ 
    $result = $dbhandle->query($query); 
    return $result; 
} 

function sqlite_fetch_array(&$result, $type) 
{ 
    #Get Columns 
    $i = 0; 
    while ($result->columnName($i)) 
    { 
        $columns[ ] = $result->columnName($i); 
        $i++; 
    } 
    
    $resx = $result->fetchArray(SQLITE3_ASSOC); 
    return $resx; 
} 

ini_set('max_execution_time', 300);
$db = new MyDB();
$db->show_tree_size();

?>

</body>
</html>
