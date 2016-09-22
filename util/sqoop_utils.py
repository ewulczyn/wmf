
import os
import sys
import argparse
import json

from db_utils import exec_hive_stat2 as exec_hive
"""
Utility for sqooping wikipedia prod tables into hive. The tables currently
supported are:
-page
-redirect
-langlinks
-pagelinks
-pageprops
-revision

Initially we just sqoop the raw data. Then we define a query to turn each table
into a canonical form,
where for each field in a table that corresponds to a page, we add the
namespace and whether it is a redirect.

"""

####################################################################
####################################################################

page_sqoop_query = """
sqoop import                                                        \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s    \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_XXXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by page_id                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                       \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                   \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  page_id,
  CAST(page_title AS CHAR(255) CHARSET utf8) AS page_title,
  page_is_redirect,
  page_namespace
FROM page 
WHERE $CONDITIONS
'  
"""  


# this is a noop, the table is already clean
clean_page_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT 
  page_id,
  page_namespace,
  page_is_redirect,
  page_title
FROM
  %(hive_db)s.%(raw_table)s
"""

####################################################################
####################################################################


redirect_sqoop_query = """
sqoop import                                                        \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_1XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by rd_from                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                        \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                          \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  rd_from,
  CAST(rd_title AS CHAR(255) CHARSET utf8) AS rd_title,
  rd_namespace
FROM redirect
WHERE $CONDITIONS
'   
"""

clean_redirect_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT 
  pfrom.page_id as rd_from_page_id,
  pfrom.page_namespace as rd_from_page_namespace,
  pfrom.page_is_redirect as rd_from_page_is_redirect,
  pfrom.page_title as rd_from_page_title,
  pto.page_id as rd_to_page_id,
  pto.page_namespace as rd_to_page_namespace,
  pto.page_is_redirect as rd_to_page_is_redirect,
  pto.page_title as rd_to_page_title
FROM
  %(hive_db)s.%(raw_table)s a 
  JOIN %(hive_db)s.%(page_table)s pfrom ON ( a.rd_from = pfrom.page_id)
  JOIN %(hive_db)s.%(page_table)s pto ON ( a.rd_title = pto.page_title AND pto.page_namespace = a.rd_namespace)
"""              

####################################################################
####################################################################

revision_sqoop_query = """
sqoop import                                                      \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by rev_parent_id                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                        \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                          \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  rev_page,
  rev_user,
  CAST(rev_user_text AS CHAR(255) CHARSET utf8) AS rev_user_text,
  rev_minor_edit,
  rev_deleted,
  rev_len,
  rev_parent_id
FROM revision
WHERE $CONDITIONS
'
"""

clean_revision_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT
  a.*,
  b.page_id as rev_page_id,
  b.page_namespace as rev_page_namespace,
  b.page_is_redirect as rev_page_is_redirect,
  b.page_title as rev_page_title
FROM
  %(hive_db)s.%(raw_table)s a JOIN %(hive_db)s.%(page_table)s b ON ( a.rev_page = b.page_id)
"""
####################################################################
####################################################################


pagelinks_sqoop_query = """
sqoop import                                                  \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                                            \
  --split-by pl_from                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                            \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                          \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  pl_from,
  CAST(pl_title AS CHAR(255) CHARSET utf8) AS pl_title,
  pl_from_namespace,
  pl_namespace
FROM pagelinks
WHERE $CONDITIONS
'
"""

# note that redirects are not resolved. In fact redirect are links and included in this table
clean_pagelinks_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT
  pfrom.page_id as pl_from_page_id,
  pfrom.page_namespace as pl_from_page_namespace,
  pfrom.page_is_redirect as pl_from_page_is_redirect,
  pfrom.page_title as pl_from_page_title,
  pto.page_id as pl_to_page_id,
  pto.page_namespace as pl_to_page_namespace,
  pto.page_is_redirect as pl_to_page_is_redirect,
  pto.page_title as pl_to_page_title
FROM
  %(hive_db)s.%(raw_table)s a 
  JOIN %(hive_db)s.%(page_table)s pfrom ON ( a.pl_from = pfrom.page_id)
  JOIN %(hive_db)s.%(page_table)s pto ON ( a.pl_title = pto.page_title AND pto.page_namespace = a.pl_namespace)
"""

####################################################################
####################################################################

page_props_sqoop_query = """
sqoop import                                                  \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by pp_page                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                            \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                          \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  pp_page,
  CAST(pp_propname AS CHAR(60) CHARSET utf8) AS pp_propname,
  CAST(pp_value AS CHAR(256) CHARSET utf8) AS pp_value
FROM page_props
WHERE $CONDITIONS
'
"""

clean_page_props_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT
  page_id,
  page_namespace,
  page_is_redirect, 
  page_title,
  pp_propname as propname,
  pp_value as value
FROM
  %(hive_db)s.%(raw_table)s a JOIN %(hive_db)s.%(page_table)s b ON ( a.pp_page = b.page_id)
"""


####################################################################
####################################################################


"""
The langlinks table is insane.
Title are represented in 'FULLPAGENAMEE style', instead of
providing a title and namespace.

Docs: https://www.mediawiki.org/wiki/Manual:Langlinks_table
Example: Kategorie:Wikipedia:Lizenzvorlage fr uufreii Dateie

This needs more parsing work. Also note that the titles use spaces instead of 
under scores
"""

#langlinks_sqoop_query = """
#sqoop import                                                      \
#  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
#  --verbose                                                         \
#  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
#  --delete-target-dir                                               \
#  --username research                                               \
#  --password-file /user/ellery/sqoop.password                       \
#  --split-by a.ll_from                                              \
#  --hive-import                                                     \
#  --hive-database %(hive_db)s                                        \
#  --create-hive-table                                               \
#  --hive-table %(result_table)s                                         \
#  --hive-delims-replacement ' '                                  \
#  --query '
#SELECT
#  ll_from,
#  regexp_replace(CAST(ll_title AS CHAR(255) CHARSET utf8), ' ', '_')  AS ll_title,
#  CAST(ll_lang AS CHAR(20) CHARSET utf8) AS ll_lang
#FROM langlinks
#WHERE $CONDITIONS
#'
#"""

####################################################################
####################################################################


queries = {
  'page' : {'sqoop': page_sqoop_query, 'clean': clean_page_query},
  'redirect': {'sqoop': redirect_sqoop_query,  'clean': clean_redirect_query},
  'pagelinks': {'sqoop': pagelinks_sqoop_query, 'clean': clean_pagelinks_query},
  'page_props': {'sqoop': page_props_sqoop_query, 'clean': clean_page_props_query},
  'revision': {'sqoop': revision_sqoop_query, 'clean': clean_revision_query},
}


def exec_sqoop(statement):
  ret =  os.system(statement)
  assert ret == 0
  return ret


def sqoop_prod_dbs(db, langs, tables):
  ret = 0

  # make this is a  priority job
  ret += os.system("export HIVE_OPTS='-hiveconf mapreduce.job.queuename=priority'")

  # create the db if it does not exist
  create_db = 'CREATE DATABASE IF NOT EXISTS %(hive_db)s;'
  params = {'hive_db':db}
  ret += exec_hive(create_db % params)

  # delete table before creating them
  delete_query = "DROP TABLE IF EXISTS %(hive_db)s.%(result_table)s; "


  # sqoop requested tables into db
  ret += os.system("export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64")

  for lang in langs:
    for table in tables:

      params = {'hive_db': db,
               'mysql_db' : lang + 'wiki', 
               'result_table': lang + '_' + table + '_raw',
               }

      ret += exec_hive(delete_query % params)
      ret += exec_sqoop(queries[table]['sqoop'] % params)



    
  # join sqooped tables with page table to get clean final table

  for lang in langs:
      for table in tables:

        params = {'hive_db': db,
               'page_table': lang + '_' + 'page' + '_raw',
               'raw_table': lang + '_' + table + '_raw',
               'result_table': lang + '_'  + table,
               }

        ret += exec_hive(delete_query % params)
        ret += exec_hive(queries[table]['clean'] % params, priority=True)


       

  assert ret ==0

if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument('--db', required = True, help='path to recommendation file' )
  parser.add_argument('--langs', required = True,  help='comma seperated list of languages' )
  parser.add_argument('--tables',  required = True )
  args = parser.parse_args()
  langs = args.langs.split(',')
  tables = args.tables.split(',') 
  db = args.db
  sqoop_prod_dbs(db, langs, tables)
