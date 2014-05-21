#!/usr/bin/perl
use strict;
use warnings;
use Test::More tests => 2;
use Test::Deep;
use Data::Dumper;

use_ok('MemcacheDBI');

my $memd_server = $ENV{'memd_server'};
my $user = $ENV{'dbi_user'};
my $password = $ENV{'dbi_pass'};
my $database = $ENV{'dbi_table'} // 'test';
my $table = $ENV{'dbi_table'} // 'test';
my $data_source = $ENV{'dbi_source'} // "dbi:CSV:f_dir=./t";

my $dbh = MemcacheDBI->connect($data_source, $user, $password, {
    'AutoCommit'         => 1,
    'ChopBlanks'         => 1,
    'ShowErrorStatement' => 1,
    'pg_enable_utf8'     => 1,
    'mysql_enable_utf8'  => 1,
});

{
local $SIG{__WARN__} = sub{}; # eat warnings about commit being on since dbi:CVS doesn't support transactions

#this is specifically to test that commit works without memcache being initialized
ok($dbh->commit, 'commit');
}
