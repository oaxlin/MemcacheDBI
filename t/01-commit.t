#!/usr/bin/perl
use strict;
use warnings;
use Test::More tests => 3;
use Test::Deep;
use Data::Dumper;

use_ok('MemcacheDBI');

my $memd_server = $ENV{'memd_server'};
my $user = $ENV{'dbi_user'};
my $password = $ENV{'dbi_pass'};
my $database = $ENV{'dbi_table'} // 'test';
my $table = $ENV{'dbi_table'} // 'test';
my $data_source = $ENV{'dbi_source'} // "dbi:CSV:f_dir=./t";

SKIP: {
    skip 'This test is designed for the DBI:Pg driver', 2 unless $data_source =~ /^DBI:Pg:/;
    foreach my $autocommit ( 0, 1 ) {
        local $SIG{__WARN__} = sub{}; # eat warnings about autocommit, it will fail with DBD:CSV
        my $dbh = eval{MemcacheDBI->connect($data_source, $user, $password, {
            'AutoCommit'         => $autocommit,
            'ChopBlanks'         => 1,
            'ShowErrorStatement' => 1,
            'pg_enable_utf8'     => 1,
            'mysql_enable_utf8'  => 1,
        })};

        #this is specifically to test that commit works without memcache being initialized
        my $test = $dbh->commit;
        ok($dbh->{'AutoCommit'} ? !$test : $test, 'commit');
    }
}
