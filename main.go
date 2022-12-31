package consumer

var GetTableFn func(string) *Table
var Redis ICache
var Etcd ICache
var Logger ILogger
