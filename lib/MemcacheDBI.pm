package MemcacheDBI;
use strict;
use warnings;
use DBI;
use vars qw( $AUTOLOAD $VERSION );
$VERSION = '0.02';
require 5.10.0;

our $DEBUG;
our $me = '[MemcacheDBI]';

=head1 NAME

MemcacheDBI - Queue memcache calls when in a dbh transaction

=head1 SYNOPSYS

MemcacheDBI is a drop in replacement for DBI.  It allows you to do trivial caching of some objects in a somewhat transactionally safe manner.

  use MemcacheDBI;
  my $dbh = MemcacheDBI->connect($data_source, $user, $password, {} ); # just like DBI
  $dbh->memd_init(\%memcache_connection_args) # see Cache::Memcached::Fast

  # Cache::Memcached::Fast should work using these calls
  $dbh->memd_get();
  $dbh->memd_set();
  $dbh->memd_commit(); # not a memcache call

  # DBI methods should all work as normal.  Additional new methods listed below
  $dbh->prepare();
  $dbh->execute();
  etc

=head1 DESCRIPTION

Attach your memcached to your DBH handle.  By doing so we can automatically queue set/get calls so that they happen at the same time as a commit.  If a rollback is issued then the queue will be cleared.

=head1 CAVEATS

As long as DBI and Memcache are both up and running your fine.  However this module will experience race conditions when one or the other goes down.  We are currently working to see if some of this can be minimized, but be aware it is impossible to protect you if the DB/Memcache servers go down. 

=head1 METHODS

=head2 memd_init

Normally you would use a MemcacheDBI->connect to create a new handle.  However if you already have a DBH handle you can use this method to create a MemcacheDBI object using your existing handle.

Accepts a the following data types

 Cache::Memcached::Fast (new Cache::Memcached::Fast)
 A DBI handle (DBI->connect)
 HASH of arguments to pass to new Cache::Memcached::Fast

=cut

sub memd_init {
    warn "[debug $DEBUG]$me->memd_init\n" if $DEBUG && $DEBUG > 3;
    my $class = shift;
    my $node = ref $class ? $class : do{ tie my %node, 'MemcacheDBI::TieDBH'; warn 'whee'; \%node; };
    while (my $handle = shift) {
        if (ref $handle eq 'DBI::db') {
            $node->{'MemcacheDBI'}->{'dbh'} = $handle;
        } elsif (ref $handle eq 'Cache::Memcached::Fast') {
            $node->{'MemcacheDBI'}->{'memd'} = $handle;
        } elsif (ref $handle eq 'HASH') {
            require Cache::Memcached::Fast;
            $node->{'MemcacheDBI'}->{'memd'} = Cache::Memcached::Fast->new($handle);
        } else {
            die "Unknown ref type.";
        }
    }
    if (! ref $class) {
        return bless $node, $class;
    }
    return $class;
}

=head2 memd_get

The same as Cache::Memcached::Fast::get

=cut

sub memd_get {
    warn "[debug $DEBUG]$me->memd_set\n" if $DEBUG && $DEBUG > 3;
    my ($self,$key) = @_;
    die 'memd not initialized'.do{my @c = caller; ' at '.$c[1].' line '.$c[2]."\n" } unless $self->{'MemcacheDBI'}->{'memd'};
    $self->{'MemcacheDBI'}->{'queue'}->{$key} // $self->{'MemcacheDBI'}->{'memd'}->get($key);
}

=head2 memd_set

The same as Cache::Memcached::Fast::set

=cut

sub memd_set {
    warn "[debug $DEBUG]$me->memd_get\n" if $DEBUG && $DEBUG > 3;
    my ($self,$key,$value) = @_;
    die 'memd not initialized'.do{my @c = caller; ' at '.$c[1].' line '.$c[2]."\n" } unless $self->{'MemcacheDBI'}->{'memd'};
    $self->{'MemcacheDBI'}->{'queue'}->{$key} = $value;
    $self->memd_commit if $self->{'AutoCommit'};
    $value;
}

=head2 memd_commit

memd_commit only commits the memcache data, if you want to commit both simply use $obj->commit instead.

=cut

sub memd_commit {
    warn "[debug $DEBUG]$me->memd_commit\n" if $DEBUG && $DEBUG > 3;
    my $self = shift;
    return 1 if ! defined $self-{'MemcacheDBI'}->{'queue'};
    die 'memd not initialized'.do{my @c = caller; ' at '.$c[1].' line '.$c[2]."\n" } unless $self->{'MemcacheDBI'}->{'memd'};
    my $queue = $self->{'MemcacheDBI'}->{'queue'};
    foreach my $key (keys %$queue) {
        $self->{'MemcacheDBI'}->{'memd'}->set($key, $queue->{$key});
    }
    delete $self->{'MemcacheDBI'}->{'queue'};
    return 1;
}

=head2 dbh_commit

dbh_commit only commits the dbh data, if you want to commit both simply use $obj->commit instead.

=cut

sub dbh_commit {
    warn "[debug $DEBUG]$me->dbh_commit\n" if $DEBUG && $DEBUG > 3;
    shift->{'MemcacheDBI'}->{'dbh'}->commit(@_);
}

=head1 DBI methods can also be used, including but not limited to:

=head2 connect

The same as DBI->connect, returns a MemcacheDBI object so you can get your additional memcache functionality

=cut

sub connect {
    warn "[debug $DEBUG]$me->connect\n" if $DEBUG && $DEBUG > 3;
    my $class = shift;
    tie my %node, 'MemcacheDBI::TieDBH';
    $node{'MemcacheDBI'}{'dbh'} = DBI->connect(@_);
    return bless \%node, $class;
}

=head2 commit

The same as DBI->commit, however it will also commit the memcached queue

=cut

sub commit {
    warn "[debug $DEBUG]$me->commit\n" if $DEBUG && $DEBUG > 3;
    my $self = shift;
    # TODO handle rolling back the memcache stuff if dbh fails
    $self->memd_commit && $self->{'MemcacheDBI'}->{'dbh'}->commit(@_);
}

=head2 rollback

The same as DBI->rollback, however it will also rollback the memcached queue

=cut

sub rollback {
    warn "[debug $DEBUG]$me->rollback\n" if $DEBUG && $DEBUG > 3;
    my $self = shift;
    delete $self->{'MemcacheDBI'}->{'queue'};
    warn 'rollback ineffective with AutoCommit enabled'.do{my @c = caller; ' at '.$c[1].' line '.$c[2]."\n" } if $self->{'AutoCommit'};
    $self->{'MemcacheDBI'}->{'dbh'}->rollback(@_);
}

sub AUTOLOAD {
    my $self = shift;
    my($field)=$AUTOLOAD;
    $field =~ s/.*://;
    my $method = (ref $self).'::'.$field;
    warn "[debug $DEBUG]$me create autoload for $method\n" if $DEBUG && $DEBUG > 1;
    no strict 'refs'; ## no critic
    *$method = sub {
        my $self = shift;
        warn "[debug $DEBUG]${me}->{'dbh'}->$field\n" if $DEBUG && $DEBUG > 3;
        $self->{'MemcacheDBI'}->{'dbh'}->$field(@_);
    };
    $self->$field(@_);
}

package MemcacheDBI::TieDBH;

sub TIEHASH {
    my $class = shift;
    return bless {MemcacheDBI=>{}}, $class;
}

sub FETCH {
    my ($self,$key) = @_;
    return $self->{'MemcacheDBI'} if ($key eq 'MemcacheDBI');
    die 'DBI not initialized.'.do{my @c = caller; ' at '.$c[1].' line '.$c[2]."\n" } unless $self->{'MemcacheDBI'}->{'dbh'};
    $self->{'MemcacheDBI'}->{'dbh'}->{$key};
}

sub STORE {
    my ($self,$key,$value) = @_;
    die 'DBI not initialized.'.do{my @c = caller; ' at '.$c[1].' line '.$c[2]."\n" } unless $self->{'MemcacheDBI'}->{'dbh'};
    $self->{'MemcacheDBI'}->{'dbh'}->{$key} = $value;
}

sub DELETE {
    my ($self,$key) = @_;
    $key eq 'MemcacheDBI' ? $self->{'MemcacheDBI'}={} : delete $self->{'MemcacheDBI'}->{'dbh'}->{$key};
}


1;

=head1 REPOSITORY

The code is available on github:

  https://github.com/oaxlin/MemcacheDBI.git

=head1 DISCLAIMER

THIS SOFTWARE IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.

