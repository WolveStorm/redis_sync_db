# redis_sync_db
redis_sync_pg

一个简单的Redis缓存中对hash操作的封装。

目的：
  1. 数据主要在redis，pg只是备份，为了更好的性能。
  2. 使用redis不需要再手动判断是否失效，若失效会自动从数据库load。
  3. 主要操作的是redis操，写redis后异步刷新到pg。
 

优化:
  1. 解决消息丢失的问题
