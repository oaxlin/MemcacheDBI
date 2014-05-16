#!/usr/bin/perl
use strict;
use warnings;
use Test::More tests => 19;
use Test::Deep;
use Data::Dumper;

use_ok('MemcacheDBI');

my $memd_server = $ENV{'memd_server'};
my $user = $ENV{'dbi_user'};
my $password = $ENV{'dbi_pass'};
my $database = $ENV{'dbi_table'} // 'test';
my $table = $ENV{'dbi_table'} // 'test';
my $data_source = $ENV{'dbi_source'} // "dbi:CSV:f_dir=./t";

my $dbh_outside = DBI->connect($data_source, $user, $password, {
    'AutoCommit'         => 1,
    'ChopBlanks'         => 1,
    'ShowErrorStatement' => 1,
    'pg_enable_utf8'     => 1,
    'mysql_enable_utf8'  => 1,
});
cmp_ok(ref $dbh_outside,'eq','DBI::db', 'DBH created successfully');


SKIP: {
    skip 'Failed to connect to database', 17 unless ref $dbh_outside;

    foreach my $AutoCommit ( 0, 1 ) {

        if (!$AutoCommit && $data_source =~ /^dbi:CSV:/) {
            #skip 'Failed to connect to database', 17 unless $user && $password && $table && $data_source;
            next;
        }

        my $testData = 'abc'.$AutoCommit;
        my $dbh = MemcacheDBI->connect($data_source, $user, $password, {
            'AutoCommit'         => $AutoCommit,
            'ChopBlanks'         => 1,
            'ShowErrorStatement' => 1,
            'pg_enable_utf8'     => 1,
            'mysql_enable_utf8'  => 1,
        });
        ok(ref $dbh, 'MemcacheDBH created successfully AutoCommit='.$AutoCommit);




        my $sth = $dbh->prepare("update $table set district = '' where locationnum = 126");
        $sth->execute;
        $dbh->commit unless $dbh->{'AutoCommit'};
        $sth = $dbh->prepare("select locationnum,custnum,district from $table where locationnum = 126");
        $sth->execute;
        my $data_dbh = $sth->fetchrow_hashref;
        cmp_ok( $data_dbh->{'locationnum'},'==', 126, "Retrieved a row");





        skip 'no memcache server configured eg "export memd_server=localhost:11211"', 15 unless $memd_server;
        SKIP2: {
            my $newdbh = $dbh->memd_init({
                servers => [$memd_server],
                namespace => 'oaxlinMemcacheDBItests:',
                close_on_error => 1,
                max_failures => 3,
                failure_timeout => 2,
            });
            ok( $dbh->{'MemcacheDBI'}->{'memd'}->server_versions , 'Memcache successfully initialized');




            ok( $dbh->memd_set('test1',$data_dbh) , 'Memcache set success');
            my $data_memd = $dbh->memd_get('test1');
            cmp_deeply($data_memd,$data_dbh, 'Memcache get success, before commit') unless $dbh->{'AutoCommit'};
            $dbh->commit unless $dbh->{'AutoCommit'};
            $data_memd = $dbh->memd_get('test1');
            cmp_deeply($data_memd,$data_dbh, 'Memcache get success');



            my $data_memd_updated = $dbh->memd_get('test1');
            $data_memd_updated->{'district'} = $testData;
            $sth = $dbh->prepare("update $table set district = ? where locationnum = 126");
            $sth->execute($testData);
            ok( $dbh->memd_set('test1',$data_memd_updated) , 'Memcache set success');

            my $data_memd2 = $dbh->memd_get('test1');
            my $data_cmp;
            if (! $dbh->{'AutoCommit'}) {
                my $data_memd3 = $dbh->memd_get('test1');
                cmp_deeply($data_memd3,$data_memd2, 'Memcache get success, before rollback');
                $dbh->rollback;
                $data_cmp = $data_memd;
            } else {
                $data_cmp = $data_memd2;
            }
            cmp_deeply($data_memd_updated,$data_memd2, 'Memcache get success');

            $sth = $dbh_outside->prepare("select locationnum,custnum,district from $table where locationnum = 126");
            $sth->execute;
            my $data_dbh_outside = $sth->fetchrow_hashref;
            cmp_deeply($data_dbh_outside,$data_cmp, 'Compare memcache to actual database');
        }


        $sth->finish;
        $dbh->disconnect;
    }
    $dbh_outside->disconnect;
}
