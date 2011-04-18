
use strict;
use Test::More;
use Test::mysqld;
use Queue::Q4M::Worker;

my $mysqld = Test::mysqld->new(
    my_cnf => {
        'skip-networking' => '',
    }
);

my $dbh = DBI->connect($mysqld->dsn, undef, undef, {
    RaiseError => 1,
    AutoCommit => 1,
});

my @deploy_stmts = (
    q|INSTALL PLUGIN queue SONAME 'libqueue_engine.so'|,
    q|CREATE FUNCTION queue_wait RETURNS INT SONAME 'libqueue_engine.so'|,
    q|CREATE FUNCTION queue_end RETURNS INT SONAME 'libqueue_engine.so'|,
    q|CREATE FUNCTION queue_abort RETURNS INT SONAME 'libqueue_engine.so'|,
    q|CREATE FUNCTION queue_rowid RETURNS INT SONAME 'libqueue_engine.so'|,
    q|CREATE FUNCTION queue_set_srcid RETURNS INT SONAME 'libqueue_engine.so'|,
    q|CREATE FUNCTION queue_compact RETURNS INT SONAME 'libqueue_engine.so'|,
    q|CREATE TABLE queue ( args TEXT NOT NULL) ENGINE=Queue|,
);
foreach my $stmt (@deploy_stmts) {
    $dbh->do( $stmt );
}

for( 1..10 ) {
    $dbh->do(<<EOSQL, undef, $_);
        INSERT INTO queue VALUES (?)
EOSQL
}

my $alrmed;
eval {
    $SIG{ALRM} = sub { $alrmed++ };
    alarm(10);
    my %rows;
    my $worker = Queue::Q4M::Worker->new(
        dbh => $dbh,
        sql => "SELECT args FROM queue WHERE queue_wait('queue', 1)",
        work_once => sub {
            my ($worker, $row) = @_;
    
            $rows{ $row->{args} }++;
            if ( keys %rows == 10 ) {
                note "Received all rows, stopping process";
                ok(1, "Received all rows");
                $worker->signal_received('INT'); # Dummy
            }
        }
    );
    $worker->work;
    alarm(0);
};
if ($@) {
    alarm(0);
    fail ("Received exception: $@");
}
    
done_testing;